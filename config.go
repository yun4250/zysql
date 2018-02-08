package zysql

import (
	"time"
	"path/filepath"
	"os"
	"fmt"
	"database/sql"
	"sync"
)

const (
	DisableBackUp = 0 //never do backup
	CacheBackUp   = 1 //backup only system exits or commit failed
	DiskBackUp    = 2 //backup every msg saved
)

var (
	dbs  = make(map[string]*sql.DB)
	lock sync.Mutex
)

//only check once
type Config struct {
	Ip                  []string
	Port                int
	DataBase            string
	DataSourceGenerator func(ip string, port int, database string) string
	DriverName          string
	Sql                 string
	Parser func(s string) ([]interface{}, error)

	MaxInterval  time.Duration
	MaxBatchSize uint
	MaxThreads   uint
	MaxRetry     uint

	BackUpLevel      int8
	BackUpPath       string
	BackUpFilePrefix string

	EnableStatistics bool

	config *Config
}

func (config *Config) check() error {
	if config.config == nil {
		if config.Ip == nil || len(config.Ip) == 0 {
			return fmt.Errorf("SqlConfig: find no Ip address")
		}
		if config.Port == 0 {
			return fmt.Errorf("SqlConfig: Port not set")
		}
		if config.DataBase == "" {
			return fmt.Errorf("SqlConfig: Database not set")
		}
		if config.DriverName == "" {
			return fmt.Errorf("SqlConfig: DriverName not set")
		}
		if config.DataSourceGenerator == nil {
			return fmt.Errorf("SqlConfig: DataSourceGenerator not set")
		}
		if config.Sql == "" {
			return fmt.Errorf("SqlConfig: Sql not set")
		}
		if config.Parser == nil {
			return fmt.Errorf("SqlConfig: Parser not set")
		}
		if config.MaxThreads == 0 {
			config.MaxThreads = 5
		}
		if config.BackUpLevel > DisableBackUp {
			if config.BackUpPath == "" {
				config.BackUpPath = "backup"
			}
			if config.BackUpFilePrefix == "" {
				config.BackUpFilePrefix = config.DataBase
			}
			absPath, _ := filepath.Abs(config.BackUpPath)
			if err := os.MkdirAll(absPath, 0777); err != nil {
				return fmt.Errorf("SqlConfig: can not find BackUpPath - %s", err.Error())
			}
			config.BackUpPath = absPath
		}
		if config.MaxInterval > time.Second*58 {
			config.MaxInterval = time.Second * 58
		}
		if config.MaxBatchSize == 0 {
			config.MaxBatchSize = 1
			config.MaxInterval = 0
		}
		config.config = &Config{
			Ip:                  config.Ip,
			Port:                config.Port,
			DataBase:            config.DataBase,
			DriverName:          config.DriverName,
			DataSourceGenerator: config.DataSourceGenerator,
			Sql:                 config.Sql,
			Parser:              config.Parser,
			MaxRetry:            config.MaxRetry,
			MaxInterval:         config.MaxInterval,
			MaxBatchSize:        config.MaxBatchSize,
			MaxThreads:          config.MaxThreads,
			BackUpLevel:         config.BackUpLevel,
			BackUpPath:          config.BackUpPath,
			BackUpFilePrefix:    config.BackUpFilePrefix,
			EnableStatistics:    config.EnableStatistics,
		}
	}
	return nil
}
