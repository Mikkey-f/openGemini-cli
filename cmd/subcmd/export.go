//go:build !offline
// +build !offline

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
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/vbauerster/mpb/v7"
)

var (
	MpbProgress         = mpb.New(mpb.WithWidth(100))
	ResumeJsonPath      string
	ProgressedFilesPath string
)

func (c *ExportCommand) runOnlineMode(config *ExportConfig) error {
	maxRetries := 3
	retryDelay := 5 * time.Second

	for i := 0; i < maxRetries; i++ {
		err := c.runOnlineModeInternal(config)
		if err == nil {
			return nil
		}

		if isConnectionError(err) {
			if i < maxRetries-1 {
				c.exportCmd.defaultLogger.Printf("Connection failed, retrying in %v... (attempt %d/%d)",
					retryDelay, i+1, maxRetries)
				time.Sleep(retryDelay)
				continue
			}

			if config.DataDir != "" || config.WalDir != "" {
				return fmt.Errorf("online mode failed after %d retries. Offline mode is not available in this build. Please compile with -tags offline to enable offline export: %w", maxRetries, err)
			}

			return fmt.Errorf("online mode failed after %d retries: %w", maxRetries, err)
		}

		return err
	}

	return fmt.Errorf("failed after %d retries", maxRetries)
}

func (c *ExportCommand) runOnlineModeInternal(config *ExportConfig) error {
	if config.DBFilter == "" {
		return fmt.Errorf("export flag dbfilter is required")
	}
	if config.Format != remoteFormatExporter && config.Out == "" {
		return fmt.Errorf("export flag out is required")
	}

	onlineExporter := NewOnlineExporter(c.exportCmd)
	ctx := context.Background()
	return onlineExporter.Export(ctx, config, nil)
}

func (c *ExportCommand) runOnlineModeWithResume(config *ExportConfig, progressedFiles map[string]struct{}) error {
	if config.DBFilter == "" {
		return fmt.Errorf("export flag dbfilter is required")
	}
	if config.Format != remoteFormatExporter && config.Out == "" {
		return fmt.Errorf("export flag out is required")
	}

	onlineExporter := NewOnlineExporter(c.exportCmd)
	ctx := context.Background()
	return onlineExporter.Export(ctx, config, progressedFiles)
}

func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	if strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "network") ||
		strings.Contains(errStr, "dial") {
		return true
	}

	if _, ok := err.(net.Error); ok {
		return true
	}

	return false
}

func (c *ExportCommand) Run(config *ExportConfig) error {
	if err := flag.CommandLine.Parse([]string{"-loggerLevel=ERROR"}); err != nil {
		return err
	}
	c.cfg = config
	c.exportCmd = NewExporter()

	return c.process()
}

func (c *ExportCommand) process() error {
	useOffline := c.cfg.DataDir != "" || c.cfg.WalDir != ""

	if c.cfg.Resume {
		if err := ReadLatestProgressFile(); err != nil {
			return err
		}
		oldConfig, err := getResumeConfig(c.cfg)
		if err != nil {
			return err
		}
		progressedFiles, err := getProgressedFiles()
		if err != nil {
			return err
		}

		isOnlineResume := false
		for path := range progressedFiles {
			if strings.HasPrefix(path, "online://") {
				isOnlineResume = true
				break
			}
		}

		if isOnlineResume && !useOffline {
			return c.runOnlineModeWithResume(oldConfig, progressedFiles)
		} else {
			return fmt.Errorf("offline mode is not available in this build. Please compile with -tags offline to enable offline export")
		}
	} else {
		if err := CreateNewProgressFolder(); err != nil {
			return err
		}

		if useOffline {
			return fmt.Errorf("offline mode is not available in this build. Please compile with -tags offline to enable offline export")
		} else {
			return c.runOnlineMode(c.cfg)
		}
	}
}

func getResumeConfig(options *ExportConfig) (*ExportConfig, error) {
	jsonData, err := os.ReadFile(ResumeJsonPath)
	if err != nil {
		return nil, err
	}
	var config ExportConfig
	err = json.Unmarshal(jsonData, &config)
	if err != nil {
		return nil, err
	}
	config.Resume = true
	config.RemoteUsername = options.RemoteUsername
	config.RemotePassword = options.RemotePassword
	return &config, nil
}

func getProgressedFiles() (map[string]struct{}, error) {
	file, err := os.Open(ProgressedFilesPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineSet := make(map[string]struct{})

	for scanner.Scan() {
		line := scanner.Text()
		lineSet[line] = struct{}{}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lineSet, nil
}

// CreateNewProgressFolder init ResumeJsonPath and ProgressedFilesPath
func CreateNewProgressFolder() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	targetPath := filepath.Join(home, ".ts-cli", time.Now().Format("2006-01-02_15-04-05.000000000"))
	err = os.MkdirAll(targetPath, os.ModePerm)
	if err != nil {
		return err
	}
	// create progress.json
	progressJson := filepath.Join(targetPath, "progress.json")
	ResumeJsonPath = progressJson
	// create progressedFiles
	progressedFiles := filepath.Join(targetPath, "progressedFiles")
	ProgressedFilesPath = progressedFiles
	return nil
}

// ReadLatestProgressFile reads and processes the latest folder
func ReadLatestProgressFile() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	baseDir := filepath.Join(home, ".ts-cli")
	var dirs []string
	err = filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() || path == baseDir {
			return nil
		}
		dirs = append(dirs, path)
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(dirs)
	latestDir := dirs[len(dirs)-1]
	// read progress.json
	ResumeJsonPath = filepath.Join(latestDir, "progress.json")
	// read progressedFiles
	ProgressedFilesPath = filepath.Join(latestDir, "progressedFiles")
	return nil
}

func (d *dataFilter) parseTime(clc *ExportConfig) error {
	var start, end string
	timeSlot := strings.Split(clc.TimeFilter, "~")
	if len(timeSlot) == 2 {
		start = timeSlot[0]
		end = timeSlot[1]
	} else if clc.TimeFilter != "" {
		return fmt.Errorf("invalid time filter %q", clc.TimeFilter)
	}

	if start != "" {
		st, err := convertTime(start)
		if err != nil {
			return err
		}
		d.startTime = st
	}

	if end != "" {
		ed, err := convertTime(end)
		if err != nil {
			return err
		}
		d.endTime = ed
	}

	if d.startTime > d.endTime {
		return fmt.Errorf("start time `%q` > end time `%q`", start, end)
	}

	return nil
}

func (d *dataFilter) parseDatabase(dbFilter string) {
	if dbFilter == "" {
		return
	}
	d.database = dbFilter
}

func (d *dataFilter) parseRetention(retentionFilter string) {
	if retentionFilter == "" {
		return
	}
	d.retention = retentionFilter
}

func (d *dataFilter) parseMeasurement(mstFilter string) error {
	if mstFilter == "" {
		return nil
	}
	if mstFilter != "" && d.database == "" {
		return fmt.Errorf("measurement filter %q requires database filter", mstFilter)
	}
	d.measurement = mstFilter
	return nil
}

// timeFilter [startTime, endTime]
func (d *dataFilter) timeFilter(t int64) bool {
	return t >= d.startTime && t <= d.endTime
}

type txtParser struct{}

func newTxtParser() *txtParser {
	return &txtParser{}
}

// parse2SeriesKeyWithoutVersion parse encoded index key to line protocol series key,without version and escape special characters
// encoded index key format: [total len][ms len][ms][tagkey1 len][tagkey1 val]...]
// parse to line protocol format: mst,tagkey1=tagval1,tagkey2=tagval2...
func (t *txtParser) parse2SeriesKeyWithoutVersion(key []byte, dst []byte, splitWithNull bool, point *opengemini.Point) ([]byte, error) {
	msName, src, err := influx.MeasurementName(key)
	originMstName := influx.GetOriginMstName(string(msName))
	originMstName = EscapeMstName(originMstName)
	if err != nil {
		return []byte{}, err
	}
	var split [2]byte
	if splitWithNull {
		split[0], split[1] = influx.ByteSplit, influx.ByteSplit
	} else {
		split[0], split[1] = '=', ','
	}
	point.Measurement = originMstName
	dst = append(dst, originMstName...)
	dst = append(dst, ',')
	tagsN := encoding.UnmarshalUint16(src)
	src = src[2:]
	var i uint16
	for i = 0; i < tagsN; i++ {
		keyLen := encoding.UnmarshalUint16(src)
		src = src[2:]
		tagKey := EscapeTagKey(string(src[:keyLen]))
		dst = append(dst, tagKey...)
		dst = append(dst, split[0])
		src = src[keyLen:]

		valLen := encoding.UnmarshalUint16(src)
		src = src[2:]
		tagVal := EscapeTagValue(string(src[:valLen]))
		dst = append(dst, tagVal...)
		dst = append(dst, split[1])
		src = src[valLen:]

		point.AddTag(tagKey, tagVal)
	}
	return dst[:len(dst)-1], nil
}

func (t *txtParser) appendFields(rec record.Record, buf []byte, point *opengemini.Point) ([]byte, error) {
	buf = append(buf, ' ')
	for i, field := range rec.Schema {
		if field.Name == "time" {
			continue
		}
		buf = append(buf, EscapeFieldKey(field.Name)+"="...)
		switch field.Type {
		case influx.Field_Type_Float:
			buf = strconv.AppendFloat(buf, rec.Column(i).FloatValues()[0], 'g', -1, 64)
			point.AddField(EscapeFieldKey(field.Name), strconv.FormatFloat(rec.Column(i).FloatValues()[0], 'g', -1, 64))
		case influx.Field_Type_Int:
			buf = strconv.AppendInt(buf, rec.Column(i).IntegerValues()[0], 10)
			point.AddField(EscapeFieldKey(field.Name), strconv.FormatInt(rec.Column(i).IntegerValues()[0], 10))
		case influx.Field_Type_Boolean:
			buf = strconv.AppendBool(buf, rec.Column(i).BooleanValues()[0])
			point.AddField(EscapeFieldKey(field.Name), strconv.FormatBool(rec.Column(i).BooleanValues()[0]))
		case influx.Field_Type_String:
			var str []string
			str = rec.Column(i).StringValues(str)
			buf = append(buf, '"')
			buf = append(buf, EscapeStringFieldValue(str[0])...)
			buf = append(buf, '"')
			point.AddField(EscapeFieldKey(field.Name), str[0])
		default:
			// This shouldn't be possible, but we'll format it anyway.
			buf = append(buf, fmt.Sprintf("%v", rec.Column(i))...)
			point.AddField(EscapeFieldKey(field.Name), fmt.Sprintf("%v", rec.Column(i)))
		}
		if i != rec.Len()-2 {
			buf = append(buf, ',')
		} else {
			buf = append(buf, ' ')
		}
	}
	buf = strconv.AppendInt(buf, rec.Times()[0], 10)
	buf = append(buf, '\n')
	point.Timestamp = rec.Times()[0] // point.Time = time.Unix(0, rec.Times()[0])
	return buf, nil
}

func (t *txtParser) writeMstInfoFromTssp(_ io.Writer, _ io.Writer, _ string, _ bool, _ interface{}) error {
	// This function is only used in offline mode
	return nil
}

func (t *txtParser) writeMstInfoFromWal(_ io.Writer, _ io.Writer, _ interface{}, _ string) error {
	return nil
}

func (t *txtParser) getRowBuf(buf []byte, measurementName string, row interface{}, point *opengemini.Point) ([]byte, error) {
	rowData, ok := row.(influx.Row)
	if !ok {
		return nil, fmt.Errorf("invalid row type")
	}
	point.Measurement = measurementName
	tags := rowData.Tags
	fields := rowData.Fields
	tm := rowData.Timestamp

	buf = append(buf, measurementName...)
	buf = append(buf, ',')
	for i, tag := range tags {
		buf = append(buf, EscapeTagKey(tag.Key)+"="...)
		buf = append(buf, EscapeTagValue(tag.Value)...)
		if i != len(tags)-1 {
			buf = append(buf, ',')
		} else {
			buf = append(buf, ' ')
		}
		point.AddTag(EscapeTagKey(tag.Key), EscapeTagValue(tag.Value))
	}
	for i, field := range fields {
		buf = append(buf, EscapeFieldKey(field.Key)+"="...)
		switch field.Type {
		case influx.Field_Type_Float:
			buf = strconv.AppendFloat(buf, field.NumValue, 'g', -1, 64)
			point.AddField(EscapeFieldKey(field.Key), strconv.FormatFloat(field.NumValue, 'g', -1, 64))
		case influx.Field_Type_Int:
			buf = strconv.AppendInt(buf, int64(field.NumValue), 10)
			point.AddField(EscapeFieldKey(field.Key), strconv.FormatInt(int64(field.NumValue), 10))
		case influx.Field_Type_Boolean:
			buf = strconv.AppendBool(buf, field.NumValue == 1)
			point.AddField(EscapeFieldKey(field.Key), strconv.FormatBool(field.NumValue == 1))
		case influx.Field_Type_String:
			buf = append(buf, '"')
			buf = append(buf, EscapeStringFieldValue(field.StrValue)...)
			buf = append(buf, '"')
			point.AddField(EscapeFieldKey(field.Key), field.StrValue)
		default:
			// This shouldn't be possible, but we'll format it anyway.
			buf = append(buf, fmt.Sprintf("%v", field)...)
			point.AddField(EscapeFieldKey(field.Key), fmt.Sprintf("%v", field))
		}
		if i != len(fields)-1 {
			buf = append(buf, ',')
		} else {
			buf = append(buf, ' ')
		}
	}
	buf = strconv.AppendInt(buf, tm, 10)
	buf = append(buf, '\n')
	point.Timestamp = tm // point.Time = time.Unix(0, tm)
	return buf, nil
}

func (t *txtParser) writeMetaInfo(metaWriter io.Writer, infoType InfoType, info string) {
	switch infoType {
	case InfoTypeDatabase:
		fmt.Fprintf(metaWriter, "# CONTEXT-DATABASE: %s\n", info)
	case InfoTypeRetentionPolicy:
		fmt.Fprintf(metaWriter, "# CONTEXT-RETENTION-POLICY: %s\n", info)
	case InfoTypeMeasurement:
		fmt.Fprintf(metaWriter, "# CONTEXT-MEASUREMENT: %s\n", info)
	default:
		fmt.Fprintf(metaWriter, "%s\n", info)
	}
}

func (t *txtParser) writeOutputInfo(outputWriter io.Writer, info string) {
	fmt.Fprint(outputWriter, info)
}

type csvParser struct {
	fieldsName     map[string]map[string][]string // database -> measurement -> []field
	curDatabase    string
	curMeasurement string
}

func newCsvParser() *csvParser {
	return &csvParser{
		fieldsName: make(map[string]map[string][]string),
	}
}

// parse2SeriesKeyWithoutVersion parse encoded index key to csv series key,without version and escape special characters
// encoded index key format: [total len][ms len][ms][tagkey1 len][tagkey1 val]...]
// parse to csv format: mst,tagval1,tagval2...
func (c *csvParser) parse2SeriesKeyWithoutVersion(key []byte, dst []byte, splitWithNull bool, _ *opengemini.Point) ([]byte, error) {
	_, src, err := influx.MeasurementName(key)
	if err != nil {
		return []byte{}, err
	}
	var split [2]byte
	if splitWithNull {
		split[0], split[1] = influx.ByteSplit, influx.ByteSplit
	} else {
		split[0], split[1] = '=', ','
	}

	tagsN := encoding.UnmarshalUint16(src)
	src = src[2:]
	var i uint16
	for i = 0; i < tagsN; i++ {
		keyLen := encoding.UnmarshalUint16(src)
		src = src[2:]
		src = src[keyLen:]

		valLen := encoding.UnmarshalUint16(src)
		src = src[2:]
		tagVal := EscapeTagValue(string(src[:valLen]))
		dst = append(dst, tagVal...)
		dst = append(dst, split[1])
		src = src[valLen:]
	}
	return dst, nil

}

func (c *csvParser) appendFields(rec record.Record, buf []byte, _ *opengemini.Point) ([]byte, error) {
	curFieldsName := c.fieldsName[c.curDatabase][c.curMeasurement]
	for _, fieldName := range curFieldsName {
		if fieldName == "time" {
			continue
		}
		k, ok := getFieldNameIndexFromRecord(rec.Schema, fieldName)
		if !ok {
			buf = append(buf, ',')
		} else {
			switch rec.Schema[k].Type {
			case influx.Field_Type_Float:
				buf = strconv.AppendFloat(buf, rec.Column(k).FloatValues()[0], 'g', -1, 64)
			case influx.Field_Type_Int:
				buf = strconv.AppendInt(buf, rec.Column(k).IntegerValues()[0], 10)
			case influx.Field_Type_Boolean:
				buf = strconv.AppendBool(buf, rec.Column(k).BooleanValues()[0])
			case influx.Field_Type_String:
				var str []string
				str = rec.Column(k).StringValues(str)
				buf = append(buf, '"')
				buf = append(buf, EscapeStringFieldValue(str[0])...)
				buf = append(buf, '"')
			default:
				// This shouldn't be possible, but we'll format it anyway.
				buf = append(buf, fmt.Sprintf("%v", rec.Column(k))...)
			}
			if k != rec.Len()-1 {
				buf = append(buf, ',')
			}
		}
	}
	buf = strconv.AppendInt(buf, rec.Times()[0], 10)
	buf = append(buf, '\n')
	return buf, nil
}

func (c *csvParser) writeMstInfoFromTssp(_ io.Writer, _ io.Writer, _ string, _ bool, _ interface{}) error {
	// This function is only used in offline mode
	return nil
}

func (c *csvParser) writeMstInfoFromWal(metaWriter io.Writer, outputWriter io.Writer, row interface{}, currentDatabase string) error {
	rowData, ok := row.(influx.Row)
	if !ok {
		return fmt.Errorf("invalid row type")
	}
	tagsN := rowData.Tags
	fieldsN := rowData.Fields
	var tags, fields, tagsType, fieldsType []string
	for _, tag := range tagsN {
		tags = append(tags, tag.Key)
		tagsType = append(tagsType, "tag")
	}
	for _, field := range fieldsN {
		fields = append(fields, field.Key)
		fieldsType = append(fieldsType, influx.FieldTypeString(field.Type))
	}
	fieldsType = append(fieldsType, "dateTime:timeStamp")
	measurementWithVersion := rowData.Name
	measurementName := influx.GetOriginMstName(measurementWithVersion)
	measurementName = EscapeMstName(measurementName)
	c.fieldsName[currentDatabase] = make(map[string][]string)
	c.fieldsName[currentDatabase][measurementName] = fields
	c.curDatabase = currentDatabase
	c.curMeasurement = measurementName
	// write datatype
	fmt.Fprintf(metaWriter, "#datatype %s,%s\n", strings.Join(tagsType, ","), strings.Join(fieldsType, ","))
	// write tags and fields name
	buf := influx.GetBytesBuffer()
	defer influx.PutBytesBuffer(buf)
	buf = append(buf, strings.Join(tags, ",")...)
	buf = append(buf, ',')
	buf = append(buf, strings.Join(fields, ",")...)
	buf = append(buf, ',')
	buf = append(buf, "time"...)
	buf = append(buf, '\n')
	_, err := outputWriter.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (c *csvParser) getRowBuf(buf []byte, measurementName string, row interface{}, _ *opengemini.Point) ([]byte, error) {
	rowData, ok := row.(influx.Row)
	if !ok {
		return nil, fmt.Errorf("invalid row type")
	}
	tags := rowData.Tags
	fields := rowData.Fields
	tm := rowData.Timestamp

	for _, tag := range tags {
		buf = append(buf, EscapeTagValue(tag.Value)...)
		buf = append(buf, ',')
	}
	curFieldsName := c.fieldsName[c.curDatabase][c.curMeasurement]
	for _, fieldName := range curFieldsName {
		if fieldName == "time" {
			continue
		}
		k, ok := getFieldNameIndexFromRow(fields, fieldName)
		if !ok {
			buf = append(buf, ',')
		} else {
			switch fields[k].Type {
			case influx.Field_Type_Float:
				buf = strconv.AppendFloat(buf, fields[k].NumValue, 'g', -1, 64)
			case influx.Field_Type_Int:
				buf = strconv.AppendInt(buf, int64(fields[k].NumValue), 10)
			case influx.Field_Type_Boolean:
				buf = strconv.AppendBool(buf, fields[k].NumValue == 1)
			case influx.Field_Type_String:
				buf = append(buf, '"')
				buf = append(buf, EscapeStringFieldValue(fields[k].StrValue)...)
				buf = append(buf, '"')
			default:
				// This shouldn't be possible, but we'll format it anyway.
				buf = append(buf, fmt.Sprintf("%v", fields[k])...)
			}
			buf = append(buf, ',')
		}
	}
	buf = strconv.AppendInt(buf, tm, 10)
	buf = append(buf, '\n')
	return buf, nil
}

func (c *csvParser) writeMetaInfo(metaWriter io.Writer, infoType InfoType, info string) {
	switch infoType {
	case InfoTypeDatabase:
		fmt.Fprintf(metaWriter, "#constant database,%s\n", info)
	case InfoTypeRetentionPolicy:
		fmt.Fprintf(metaWriter, "#constant retention_policy,%s\n", info)
	case InfoTypeMeasurement:
		fmt.Fprintf(metaWriter, "#constant measurement,%s\n", info)
	default:
		return
	}
}

func (c *csvParser) writeOutputInfo(_ io.Writer, _ string) {
}

func (re *remoteExporter) Init(clc *ExportConfig) error {
	if len(clc.Remote) == 0 {
		return fmt.Errorf("execute -export cmd, using remote format, --remote is required")
	}
	h, p, err := net.SplitHostPort(clc.Remote)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(p)
	if err != nil {
		return fmt.Errorf("invalid port number :%s", err)
	}
	var authConfig *opengemini.AuthConfig
	if clc.RemoteUsername != "" {
		authConfig = &opengemini.AuthConfig{
			AuthType: 0,
			Username: clc.RemoteUsername,
			Password: clc.RemotePassword,
		}
	} else {
		authConfig = nil
	}
	var remoteConfig *opengemini.Config
	if clc.RemoteSsl {
		remoteConfig = &opengemini.Config{
			Addresses: []opengemini.Address{
				{
					Host: h,
					Port: port,
				},
			},
			AuthConfig: authConfig,
			TlsConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	} else {
		remoteConfig = &opengemini.Config{
			Addresses: []opengemini.Address{
				{
					Host: h,
					Port: port,
				},
			},
			AuthConfig: authConfig,
		}
	}

	cli, err := opengemini.NewClient(remoteConfig)
	if err != nil {
		return err
	}
	re.isExist = true
	re.client = cli
	if err = re.client.Ping(0); err != nil {
		return err
	}
	return nil
}

func (re *remoteExporter) createDatabase(dbName string) error {
	err := re.client.CreateDatabase(dbName)
	if err != nil {
		return fmt.Errorf("error writing command: %s", err)
	}
	return nil
}

func (re *remoteExporter) createRetentionPolicy(dbName string, rpName string) error {
	err := re.client.CreateRetentionPolicy(dbName, opengemini.RpConfig{
		Name:     rpName,
		Duration: "0s",
	}, false)
	if err != nil {
		return fmt.Errorf("error writing command: %s", err)
	}
	return nil
}

func (re *remoteExporter) writeAllPoints() error {
	err := re.client.WriteBatchPointsWithRp(context.Background(), re.database, re.retentionPolicy, re.points)
	if err != nil {
		return err
	}
	re.points = re.points[:0]
	return nil
}

var escapeFieldKeyReplacer = strings.NewReplacer(`,`, `\,`, `=`, `\=`, ` `, `\ `)
var escapeTagKeyReplacer = strings.NewReplacer(`,`, `\,`, `=`, `\=`, ` `, `\ `)
var escapeTagValueReplacer = strings.NewReplacer(`,`, `\,`, `=`, `\=`, ` `, `\ `)
var escapeMstNameReplacer = strings.NewReplacer(`=`, `\=`, ` `, `\ `)
var escapeStringFieldReplacer = strings.NewReplacer(`"`, `\"`, `\`, `\\`)

// EscapeFieldKey returns a copy of in with any comma or equal sign or space
// with escaped values.
func EscapeFieldKey(in string) string {
	return escapeFieldKeyReplacer.Replace(in)
}

// EscapeStringFieldValue returns a copy of in with any double quotes or
// backslashes with escaped values.
func EscapeStringFieldValue(in string) string {
	return escapeStringFieldReplacer.Replace(in)
}

// EscapeTagKey returns a copy of in with any "comma" or "equal sign" or "space"
// with escaped values.
func EscapeTagKey(in string) string {
	return escapeTagKeyReplacer.Replace(in)
}

// EscapeTagValue returns a copy of in with any "comma" or "equal sign" or "space"
// with escaped values
func EscapeTagValue(in string) string {
	return escapeTagValueReplacer.Replace(in)
}

// EscapeMstName returns a copy of in with any "equal sign" or "space"
// with escaped values.
func EscapeMstName(in string) string {
	return escapeMstNameReplacer.Replace(in)
}

// getFieldNameIndexFromRecord returns the index of a field in a slice
func getFieldNameIndexFromRecord(slice []record.Field, str string) (int, bool) {
	for i, v := range slice {
		if v.Name == str {
			return i, true
		}
	}
	return 0, false
}

func getFieldNameIndexFromRow(slice []influx.Field, str string) (int, bool) {
	for i, v := range slice {
		if v.Key == str {
			return i, true
		}
	}
	return 0, false
}

func convertTime(input string) (int64, error) {
	t, err := time.Parse(time.RFC3339, input)
	if err == nil {
		return t.UnixNano(), nil
	}

	timestamp, err := strconv.ParseInt(input, 10, 64)
	if err == nil {
		return timestamp, nil
	}

	return 0, err
}

// writeProgressJson writes progress to json file (for online mode)
func (e *Exporter) writeProgressJson(clc *ExportConfig) error {
	output, err := json.MarshalIndent(clc, "", "\t")
	if err != nil {
		return err
	}
	err = os.WriteFile(ResumeJsonPath, output, 0644)
	if err != nil {
		return err
	}
	return nil
}

// writeProgressedFiles writes progressed file name (for online mode)
func (e *Exporter) writeProgressedFiles(filename string) error {
	file, err := os.OpenFile(ProgressedFilesPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(filename + "\n")
	if err != nil {
		return err
	}
	return nil
}
