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
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/openGemini/openGemini-cli/core"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/vbauerster/mpb/v7"
)

type ExportConfig struct {
	*core.CommandLineConfig
	Export            bool
	Format            string `json:"format"`
	Out               string `json:"out"`
	DataDir           string `json:"data"`
	WalDir            string `json:"wal"`
	Remote            string `json:"remote"`
	RemoteUsername    string `json:"-"`
	RemotePassword    string `json:"-"`
	RemoteSsl         bool   `json:"remotessl"`
	DBFilter          string `json:"dbfilter"`
	RetentionFilter   string `json:"retentionfilter"`
	MeasurementFilter string `json:"mstfilter"`
	TimeFilter        string `json:"timefilter"`
	Compress          bool   `json:"compress"`
	Resume            bool
}

type ExportCommand struct {
	cfg       *ExportConfig
	exportCmd *Exporter
}

type Exporter struct {
	exportFormat      string
	databaseDiskInfos []*DatabaseDiskInfo
	filesTotalCount   int
	actualDataPath    string
	actualWalPath     string
	outPutPath        string
	filter            *dataFilter
	compress          bool
	lineCount         uint64
	resume            bool
	progress          map[string]struct{}
	remote            string
	remoteExporter    *remoteExporter
	parser

	stderrLogger  *log.Logger
	stdoutLogger  *log.Logger
	defaultLogger *log.Logger

	manifest                        map[string]struct{}
	rpNameToMeasurementTsspFilesMap map[string]map[string][]string
	rpNameToIdToIndexMap            map[string]map[uint64]interface{} // 在offline模式下为*tsi.MergeSetIndex，在online模式下为nil
	rpNameToWalFilesMap             map[string][]string

	Stderr io.Writer
	Stdout io.Writer
	bar    *mpb.Bar
}

type DatabaseDiskInfo struct {
	dbName          string
	rps             map[string]struct{}
	dataDir         string
	walDir          string
	rpToTsspDirMap  map[string]string
	rpToWalDirMap   map[string]string
	rpToIndexDirMap map[string]string
}

func (d *DatabaseDiskInfo) init(actualDataDir string, actualWalDir string, databaseName string, retentionPolicy string) error {
	d.dbName = databaseName

	// check whether the database is in actualDataPath
	dataDir := filepath.Join(actualDataDir, databaseName)
	if _, err := os.Stat(dataDir); err != nil {
		return err
	}
	// check whether the database is in actualWalPath
	walDir := filepath.Join(actualWalDir, databaseName)
	if _, err := os.Stat(walDir); err != nil {
		return err
	}

	// ie. /tmp/openGemini/data/data/my_db  /tmp/openGemini/data/wal/my_db
	d.dataDir, d.walDir = dataDir, walDir

	ptDirs, err := os.ReadDir(d.dataDir)
	if err != nil {
		return err
	}
	for _, ptDir := range ptDirs {
		// ie. /tmp/openGemini/data/data/my_db/0
		ptTsspPath := filepath.Join(d.dataDir, ptDir.Name())
		// ie. /tmp/openGemini/data/wal/my_db/0
		ptWalPath := filepath.Join(d.walDir, ptDir.Name())

		if retentionPolicy != "" {
			ptWithRp := ptDir.Name() + ":" + retentionPolicy
			// ie. /tmp/openGemini/data/data/my_db/0/autogen
			rpTsspPath := filepath.Join(ptTsspPath, retentionPolicy)
			if _, err := os.Stat(rpTsspPath); err != nil {
				return fmt.Errorf("retention policy %q invalid : %s", retentionPolicy, err)
			} else {
				d.rps[ptWithRp] = struct{}{}
				d.rpToTsspDirMap[ptWithRp] = rpTsspPath
				d.rpToIndexDirMap[ptWithRp] = filepath.Join(rpTsspPath, "index")
			}
			// ie. /tmp/openGemini/data/wal/my_db/0/autogen
			rpWalPath := filepath.Join(ptWalPath, retentionPolicy)
			if _, err := os.Stat(rpWalPath); err != nil {
				return fmt.Errorf("retention policy %q invalid : %s", retentionPolicy, err)
			} else {
				d.rpToWalDirMap[ptWithRp] = rpWalPath
			}
			continue
		}

		rpTsspDirs, err1 := os.ReadDir(ptTsspPath)
		if err1 != nil {
			return err1
		}
		for _, rpDir := range rpTsspDirs {
			if !rpDir.IsDir() {
				continue
			}
			ptWithRp := ptDir.Name() + ":" + rpDir.Name()
			rpPath := filepath.Join(ptTsspPath, rpDir.Name())
			d.rps[ptWithRp] = struct{}{}
			d.rpToTsspDirMap[ptWithRp] = rpPath
			d.rpToIndexDirMap[ptWithRp] = filepath.Join(rpPath, "index")
		}

		rpWalDirs, err2 := os.ReadDir(ptWalPath)
		if err2 != nil {
			return err2
		}
		for _, rpDir := range rpWalDirs {
			ptWithRp := ptDir.Name() + ":" + rpDir.Name()
			if !rpDir.IsDir() {
				continue
			}
			rpPath := filepath.Join(ptWalPath, rpDir.Name())
			d.rpToWalDirMap[ptWithRp] = rpPath
		}
	}
	return nil
}

type dataFilter struct {
	database    string
	retention   string
	measurement string
	startTime   int64
	endTime     int64
}

func (d *dataFilter) isBelowMinTimeFilter(t int64) bool {
	return t < d.startTime
}

func (d *dataFilter) isAboveMaxTimeFilter(t int64) bool {
	return t > d.endTime
}

func newDataFilter() *dataFilter {
	return &dataFilter{
		database:    "",
		measurement: "",
		startTime:   math.MinInt64,
		endTime:     math.MaxInt64,
	}
}

func newDatabaseDiskInfo() *DatabaseDiskInfo {
	return &DatabaseDiskInfo{
		rps:             make(map[string]struct{}),
		rpToTsspDirMap:  make(map[string]string),
		rpToWalDirMap:   make(map[string]string),
		rpToIndexDirMap: make(map[string]string),
	}
}

func newRemoteExporter() *remoteExporter {
	return &remoteExporter{
		isExist: false,
	}
}

func NewExporter() *Exporter {
	return &Exporter{
		resume:   false,
		progress: make(map[string]struct{}),

		stderrLogger: log.New(os.Stderr, "export: ", log.LstdFlags),
		stdoutLogger: log.New(os.Stdout, "export: ", log.LstdFlags),

		manifest:                        make(map[string]struct{}),
		rpNameToMeasurementTsspFilesMap: make(map[string]map[string][]string),
		rpNameToIdToIndexMap:            make(map[string]map[uint64]interface{}),
		rpNameToWalFilesMap:             make(map[string][]string),
		remoteExporter:                  newRemoteExporter(),

		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

type remoteExporter struct {
	isExist         bool
	client          opengemini.Client
	database        string
	retentionPolicy string
	points          []*opengemini.Point
}

type parser interface {
	parse2SeriesKeyWithoutVersion(key []byte, dst []byte, splitWithNull bool, point *opengemini.Point) ([]byte, error)
	appendFields(rec record.Record, buf []byte, point *opengemini.Point) ([]byte, error)
	writeMstInfoFromTssp(metaWriter io.Writer, outputWriter io.Writer, filePath string, isOrder bool, index interface{}) error
	writeMstInfoFromWal(metaWriter io.Writer, outputWriter io.Writer, row interface{}, curDatabase string) error
	writeMetaInfo(metaWriter io.Writer, infoType InfoType, info string)
	writeOutputInfo(outputWriter io.Writer, info string)
	getRowBuf(buf []byte, measurementName string, row interface{}, point *opengemini.Point) ([]byte, error)
}

type InfoType int

const (
	InfoTypeDatabase InfoType = 1 + iota
	InfoTypeRetentionPolicy
	InfoTypeMeasurement
)

const (
	tsspFileExtension    = "tssp"
	walFileExtension     = "wal"
	csvFormatExporter    = "csv"
	txtFormatExporter    = "txt"
	remoteFormatExporter = "remote"
	resumeFilePrefix     = "resume_"
	dirNameSeparator     = "_"
)

func parseShardDir(shardDirName string) (uint64, int64, int64, uint64, error) {
	shardDir := strings.Split(shardDirName, dirNameSeparator)
	if len(shardDir) != 4 {
		return 0, 0, 0, 0, errno.NewError(errno.InvalidDataDir)
	}
	shardID, err := strconv.ParseUint(shardDir[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errno.NewError(errno.InvalidDataDir)
	}
	dirStartTime, err := strconv.ParseInt(shardDir[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errno.NewError(errno.InvalidDataDir)
	}
	dirEndTime, err := strconv.ParseInt(shardDir[2], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errno.NewError(errno.InvalidDataDir)
	}
	indexID, err := strconv.ParseUint(shardDir[3], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errno.NewError(errno.InvalidDataDir)
	}
	return shardID, dirStartTime, dirEndTime, indexID, nil
}

func parseIndexDir(indexDirName string) (uint64, error) {
	indexDir := strings.Split(indexDirName, dirNameSeparator)
	if len(indexDir) != 3 {
		return 0, errno.NewError(errno.InvalidDataDir)
	}

	indexID, err := strconv.ParseUint(indexDir[0], 10, 64)
	if err != nil {
		return 0, errno.NewError(errno.InvalidDataDir)
	}
	return indexID, nil
}
