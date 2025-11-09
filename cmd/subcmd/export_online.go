// Copyright 2025 openGemini Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package subcmd

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini-cli/core"
	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

type TimeWindow struct {
	Start    time.Time
	End      time.Time
	FilePath string
}

type OnlineExporter struct {
	*Exporter
	httpClient core.HttpClient
	windows    []TimeWindow
}

func NewOnlineExporter(baseExporter *Exporter) *OnlineExporter {
	return &OnlineExporter{
		Exporter: baseExporter,
	}
}

func (oe *OnlineExporter) initHttpClient(config *ExportConfig) error {
	httpClient, err := core.NewHttpClient(config.CommandLineConfig)
	if err != nil {
		return fmt.Errorf("failed to create http client: %w", err)
	}
	oe.httpClient = httpClient
	return nil
}

func calculateWindowSize(start, end time.Time) time.Duration {
	duration := end.Sub(start)

	switch {
	case duration <= 1*time.Hour:
		return 5 * time.Minute
	case duration <= 6*time.Hour:
		return 30 * time.Minute
	case duration <= 24*time.Hour:
		return 1 * time.Hour
	case duration <= 7*24*time.Hour:
		return 6 * time.Hour
	case duration <= 30*24*time.Hour:
		return 24 * time.Hour
	default:
		return 7 * 24 * time.Hour
	}
}

func parseTimeRange(timeFilter string) (time.Time, time.Time, error) {
	// If no time filter is provided, use a default large range (from Unix epoch to now)
	if timeFilter == "" {
		// Default: from Unix epoch (1970-01-01) to current time
		start := time.Unix(0, 0)
		end := time.Now()
		return start, end, nil
	}

	timeSlot := strings.Split(timeFilter, "~")
	if len(timeSlot) != 2 {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid time filter format, expected 'start~end'")
	}

	start, err := parseTimeString(timeSlot[0])
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid start time: %w", err)
	}

	end, err := parseTimeString(timeSlot[1])
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid end time: %w", err)
	}

	if start.After(end) {
		return time.Time{}, time.Time{}, fmt.Errorf("start time must be before end time")
	}

	return start, end, nil
}

func parseTimeString(input string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, input); err == nil {
		return t, nil
	}

	if timestamp, err := strconv.ParseInt(input, 10, 64); err == nil {
		return time.Unix(0, timestamp), nil
	}

	return time.Time{}, fmt.Errorf("unable to parse time: %s", input)
}

func (oe *OnlineExporter) prepareWindows(config *ExportConfig) error {
	startTime, endTime, err := parseTimeRange(config.TimeFilter)
	if err != nil {
		return err
	}

	windowSize := calculateWindowSize(startTime, endTime)
	currentTime := startTime

	for currentTime.Before(endTime) {
		windowEnd := currentTime.Add(windowSize)
		if windowEnd.After(endTime) {
			windowEnd = endTime
		}

		virtualPath := fmt.Sprintf("online://%s/%s/%s/%d-%d",
			config.DBFilter,
			config.RetentionFilter,
			config.MeasurementFilter,
			currentTime.UnixNano(),
			windowEnd.UnixNano())

		oe.windows = append(oe.windows, TimeWindow{
			Start:    currentTime,
			End:      windowEnd,
			FilePath: virtualPath,
		})

		currentTime = windowEnd
	}

	oe.filesTotalCount = len(oe.windows)
	return nil
}

func (oe *OnlineExporter) buildQuery(config *ExportConfig, window TimeWindow) string {
	var query strings.Builder

	query.WriteString("SELECT * FROM ")

	if config.MeasurementFilter != "" {
		query.WriteString(fmt.Sprintf(`"%s"`, config.MeasurementFilter))
	} else {
		query.WriteString(`/.*/`)
	}

	query.WriteString(fmt.Sprintf(" WHERE time >= '%s' AND time <= '%s'",
		window.Start.Format(time.RFC3339),
		window.End.Format(time.RFC3339)))

	return query.String()
}

func (oe *OnlineExporter) queryAndExportWindow(ctx context.Context, config *ExportConfig, window TimeWindow, outputWriter io.Writer, currentMeasurement *string) error {
	queryStr := oe.buildQuery(config, window)

	query := &opengemini.Query{
		Command:         queryStr,
		Database:        config.DBFilter,
		RetentionPolicy: config.RetentionFilter,
	}

	result, err := oe.httpClient.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	if result.Error != "" {
		return fmt.Errorf("query error: %s", result.Error)
	}

	if len(result.Results) == 0 {
		return nil
	}

	for _, res := range result.Results {
		if len(res.Series) == 0 {
			continue
		}

		for _, series := range res.Series {
			if err := oe.exportSeries(series, outputWriter, config, currentMeasurement); err != nil {
				return err
			}
		}
	}

	return nil
}

func (oe *OnlineExporter) exportSeries(series *opengemini.Series, outputWriter io.Writer, config *ExportConfig, currentMeasurement *string) error {
	measurementName := series.Name
	if config.MeasurementFilter != "" && measurementName != config.MeasurementFilter {
		return nil
	}

	// Write CONTEXT-MEASUREMENT when measurement changes
	if measurementName != *currentMeasurement {
		oe.parser.writeMetaInfo(outputWriter, InfoTypeMeasurement, measurementName)
		*currentMeasurement = measurementName
	}

	if len(series.Values) == 0 {
		return nil
	}

	columns := series.Columns
	timeIndex := -1
	for i, col := range columns {
		if col == "time" {
			timeIndex = i
			break
		}
	}

	if timeIndex == -1 {
		return fmt.Errorf("time column not found in query result")
	}

	for _, values := range series.Values {
		if len(values) <= timeIndex {
			continue
		}

		timestamp, ok := values[timeIndex].(float64)
		if !ok {
			if tsStr, ok := values[timeIndex].(string); ok {
				if t, err := time.Parse(time.RFC3339, tsStr); err == nil {
					timestamp = float64(t.UnixNano())
				} else {
					continue
				}
			} else {
				continue
			}
		}

		line, err := oe.formatLineProtocol(measurementName, series.Tags, columns, values, int64(timestamp))
		if err != nil {
			continue
		}

		if _, err := outputWriter.Write(line); err != nil {
			return err
		}

		oe.lineCount++
	}

	return nil
}

func (oe *OnlineExporter) formatLineProtocol(measurement string, tags map[string]string, columns []string, values []interface{}, timestamp int64) ([]byte, error) {
	var buf strings.Builder

	buf.WriteString(measurement)

	for k, v := range tags {
		buf.WriteString(fmt.Sprintf(",%s=%s", EscapeTagKey(k), EscapeTagValue(v)))
	}

	buf.WriteString(" ")

	firstField := true
	for i, col := range columns {
		if col == "time" {
			continue
		}

		if i >= len(values) {
			continue
		}

		if !firstField {
			buf.WriteString(",")
		}

		fieldName := EscapeFieldKey(col)
		fieldValue := values[i]

		switch v := fieldValue.(type) {
		case float64:
			buf.WriteString(fmt.Sprintf("%s=%g", fieldName, v))
		case int64:
			buf.WriteString(fmt.Sprintf("%s=%di", fieldName, v))
		case int:
			buf.WriteString(fmt.Sprintf("%s=%di", fieldName, v))
		case bool:
			buf.WriteString(fmt.Sprintf("%s=%t", fieldName, v))
		case string:
			buf.WriteString(fmt.Sprintf(`%s="%s"`, fieldName, EscapeStringFieldValue(v)))
		default:
			buf.WriteString(fmt.Sprintf(`%s="%v"`, fieldName, v))
		}

		firstField = false
	}

	buf.WriteString(fmt.Sprintf(" %d\n", timestamp))

	return []byte(buf.String()), nil
}

func (oe *OnlineExporter) createProgressBar() (*mpb.Bar, error) {
	if oe.filesTotalCount == 0 {
		return nil, fmt.Errorf("no windows to export")
	}

	bar := MpbProgress.New(int64(oe.filesTotalCount),
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding("-").Rbound("]"),
		mpb.PrependDecorators(
			decor.Name("Exporting Data:", decor.WC{W: 20, C: decor.DidentRight}),
			decor.CountersNoUnit("%d/%d", decor.WC{W: 15, C: decor.DidentRight}),
			decor.OnComplete(
				decor.AverageETA(decor.ET_STYLE_GO, decor.WC{W: 6}),
				"complete",
			),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
		),
	)

	return bar, nil
}

// writeDDL writes DDL statements for online mode
func (oe *OnlineExporter) writeDDL(outputWriter io.Writer, config *ExportConfig) error {
	oe.parser.writeMetaInfo(outputWriter, 0, "# DDL")

	if config.DBFilter != "" {
		oe.parser.writeOutputInfo(outputWriter, fmt.Sprintf("CREATE DATABASE %s\n", config.DBFilter))
	}

	if config.RetentionFilter != "" {
		oe.parser.writeOutputInfo(outputWriter, fmt.Sprintf("CREATE RETENTION POLICY %s ON %s DURATION 0s REPLICATION 1\n", config.RetentionFilter, config.DBFilter))
	} else if config.DBFilter != "" {
		// Default retention policy
		oe.parser.writeOutputInfo(outputWriter, fmt.Sprintf("CREATE RETENTION POLICY autogen ON %s DURATION 0s REPLICATION 1\n", config.DBFilter))
	}

	oe.parser.writeMetaInfo(outputWriter, 0, "")
	return nil
}

func (oe *OnlineExporter) Export(ctx context.Context, config *ExportConfig, progressedFiles map[string]struct{}) error {
	// Initialize defaultLogger if not set
	if oe.defaultLogger == nil {
		oe.defaultLogger = oe.stdoutLogger
	}

	// Initialize parser based on format
	if oe.parser == nil {
		if config.Format == txtFormatExporter || config.Format == remoteFormatExporter {
			oe.parser = newTxtParser()
		} else if config.Format == csvFormatExporter {
			oe.parser = newCsvParser()
		}
	}

	if err := oe.initHttpClient(config); err != nil {
		return err
	}

	if err := oe.prepareWindows(config); err != nil {
		return err
	}

	if config.Resume {
		oe.resume = true
		oe.progress = progressedFiles
		oe.defaultLogger.Printf("starting resume export, you have exported %d windows\n", len(oe.progress))
	}

	if err := oe.writeProgressJson(config); err != nil {
		return err
	}

	bar, err := oe.createProgressBar()
	if err != nil {
		return err
	}
	oe.bar = bar

	var outputWriter io.Writer
	if config.Format == remoteFormatExporter {
		outputWriter = io.Discard
	} else {
		if err := os.MkdirAll(filepath.Dir(config.Out), 0755); err != nil {
			return err
		}

		var outputFile *os.File
		if oe.resume {
			exportDir := filepath.Dir(config.Out)
			exportFilePath := filepath.Join(exportDir, resumeFilePrefix+time.Now().Format("2006-01-02_15-04-05.000000000")+filepath.Ext(config.Out))
			outputFile, err = os.OpenFile(exportFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		} else {
			outputFile, err = os.OpenFile(config.Out, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		}
		if err != nil {
			return err
		}
		defer outputFile.Close()

		outputWriter = outputFile

		if config.Compress {
			gzipWriter := gzip.NewWriter(outputWriter)
			defer gzipWriter.Close()
			outputWriter = gzipWriter
		}
	}

	start, end := time.Time{}, time.Time{}
	if config.TimeFilter != "" {
		start, end, _ = parseTimeRange(config.TimeFilter)
	} else {
		start, end, _ = parseTimeRange("")
	}
	startStr := start.UTC().Format(time.RFC3339)
	endStr := end.UTC().Format(time.RFC3339)
	oe.parser.writeMetaInfo(outputWriter, 0, fmt.Sprintf("# openGemini EXPORT: %s - %s", startStr, endStr))

	// Write DDL section
	if err := oe.writeDDL(outputWriter, config); err != nil {
		return err
	}

	// Write DML section header
	oe.parser.writeMetaInfo(outputWriter, 0, "# DML")
	oe.parser.writeMetaInfo(outputWriter, 0, "# FROM HTTP API")

	// Write context information
	if config.DBFilter != "" {
		oe.parser.writeMetaInfo(outputWriter, InfoTypeDatabase, config.DBFilter)
	}
	if config.RetentionFilter != "" {
		oe.parser.writeMetaInfo(outputWriter, InfoTypeRetentionPolicy, config.RetentionFilter)
	}

	oe.defaultLogger.Printf("Exporting data total %d windows\n", oe.filesTotalCount)

	var currentMeasurement string
	for _, window := range oe.windows {
		if _, ok := oe.progress[window.FilePath]; ok {
			oe.bar.Increment()
			continue
		}

		if err := oe.queryAndExportWindow(ctx, config, window, outputWriter, &currentMeasurement); err != nil {
			if writeErr := oe.writeProgressedFiles(window.FilePath); writeErr != nil {
				oe.stderrLogger.Printf("failed to write progress: %v", writeErr)
			}
			return err
		}

		if err := oe.writeProgressedFiles(window.FilePath); err != nil {
			oe.stderrLogger.Printf("failed to write progress: %v", err)
		}

		oe.bar.Increment()
	}

	MpbProgress.Wait()
	oe.defaultLogger.Printf("Summarize %d line protocol\n", oe.lineCount)

	return nil
}
