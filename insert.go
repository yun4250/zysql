package zysql

import (
	"sync"
	"fmt"
	"github.com/zyfcn/zyLog"
	"bufio"
	"os"
	"path/filepath"
	"io"
	"strings"
	"database/sql"
	"syscall"
	"os/signal"
)

func NewInsertion(logger *zylog.ZyLogger, config Config, sql string, parser func(s string) ([]interface{}, error)) *Insertion {
	defer zylog.CatchAndThrow()
	config.Sql = sql
	config.Parser = parser
	log := logger.GetChild(config.DriverName)
	if err := config.check(); err != nil {
		log.Fatal(err)
	}
	b := &Insertion{
		config: config.config,
		logger: log,
		signal: make(chan os.Signal, 1),
	}
	go func() {
		signal.Notify(b.signal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
		<-b.signal
		b.Close()
	}()
	return b
}

type Insertion struct {
	index     int
	committer []*commit
	logger    *zylog.ChildLogger
	signal    chan os.Signal

	config *Config
	sync.Mutex
}

func (i *Insertion) close() {
	i.Lock()
	defer i.Unlock()
	for _, v := range i.committer {
		v.Close()
	}
	i.committer = []*commit{}
	i.logger.Infof("insertion closed")
}

func (i *Insertion) Close() {
	i.signal <- os.Kill
}

func (b *Insertion) init() {
	if b.committer == nil {
		b.committer = []*commit{}
		lock.Lock()
		for _, ip := range b.config.Ip {
			key := fmt.Sprintf("%s-%s:%d-%s", b.config.DriverName, ip, b.config.Port, b.config.DataBase)
			var db *sql.DB
			if v, ok := dbs[key]; ok {
				db = v
			} else {
				host := b.config.DataSourceGenerator(ip, b.config.Port, b.config.DataBase)
				if connect, err := sql.Open(b.config.DriverName, host); err != nil {
					b.logger.Errorf("fail to open DB: %s - %s", host, err.Error())
					continue
				} else {
					dbs[key] = connect
					db = connect
				}
			}
			if err := db.Ping(); err == nil {
				b.logger.Infof("connect %s success", ip)
			} else {
				b.logger.Errorf("fail to ping %s: %s", ip, err.Error())
				continue
			}
			c := &commit{
				db:     db,
				config: b.config,
				logger: b.logger.Position(ip),
			}
			b.committer = append(b.committer, c)
		}
		lock.Unlock()
	}
}

func (b *Insertion) Insert(s ...string) {
	b.Lock()
	defer b.Unlock()
	b.init()
	for _, ss := range s {
		b.index = (b.index + 1) % len(b.committer)
		b.committer[b.index].Insert(ss)
	}
}

func (b *Insertion) LoadBackUp(includeCache bool) {
	b.logger.Infof("walk backup %s start", b.config.BackUpPath)
	err := filepath.Walk(b.config.BackUpPath, func(filepath string, fi os.FileInfo, err error) error {
		if err != nil {
			b.logger.Tracef("walk backup: error %s", err.Error())
			return err
		}
		if start, end := strings.Index(fi.Name(), b.config.BackUpFilePrefix+"_"), strings.Index(fi.Name(), ".dump"); start != -1 && end != -1 {
			b.logger.Infof("backup: find %s, start", filepath)
			file, _ := os.Open(filepath)
			buf := bufio.NewReader(file)
			for {
				line, _, err := buf.ReadLine()
				if err == io.EOF {
					break
				}
				b.Insert(string(line))
			}
			file.Close()
			if er := os.Remove(filepath); er == nil {
				b.logger.Tracef("backup: clear %s success", filepath)
			} else {
				b.logger.Tracef("backup: clear %s failed: %s", filepath, er.Error())
			}
		} else {
			b.logger.Tracef("backup: find %s, not backup", filepath)
		}
		return err
	})
	if err != nil {
		b.logger.Errorf("walk backup %s failed:%s", b.config.BackUpPath, err.Error())
	} else {
		b.logger.Infof("walk backup %s success", b.config.BackUpPath)
	}
}
