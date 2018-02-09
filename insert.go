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
	"errors"
	"math/rand"
	"time"
)

var ErrLessConnection = errors.New("reach MinAliveConnection")

func NewInsertion(logger *zylog.ZyLogger, config Config, sql string, parser func(s string) ([]interface{}, error)) (*Insertion, error) {
	config.Sql = sql
	config.Parser = parser
	if err := config.check(); err != nil {
		return nil, err
	}
	b := &Insertion{
		config: config.config,
		logger: logger.GetChild("insertion"),
	}
	if b.config.WaitForOsKill {
		b.signal = make(chan os.Signal, 1)
		signal.Notify(b.signal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
		go func(bb *Insertion) {
			<-bb.signal
			bb.close()
		}(b)
	}
	return b, nil
}

type Insertion struct {
	index     int
	committer []*commit
	logger    *zylog.ChildLogger
	signal    chan os.Signal

	config *Config
	sync.Mutex
}

func (i *Insertion) Close() {
	if i.signal != nil {
		i.signal <- os.Kill
	} else {
		i.close()
	}
}

func (i *Insertion) close() {
	i.Lock()
	defer i.Unlock()
	i.logger.Infof("insertion closing %d", len(i.committer))
	for _, v := range i.committer {
		v.Close()
	}
	i.committer = nil
	i.logger.Infof("insertion closed")
}

func (b *Insertion) init() error {
	if b.committer == nil {
		b.committer = []*commit{}
		lock.Lock()
		for _, ip := range b.config.Ip {
			key := fmt.Sprintf("%s-%s:%d-%s", b.config.DriverName, ip, b.config.Port, b.config.DataBase)
			var db *sql.DB
			if v, ok := dbs[key]; ok{
				if err := v.Ping(); err == nil {
					db = v
				}else{
					host := b.config.DataSourceGenerator(ip, b.config.Port, b.config.DataBase)
					if connect, err := sql.Open(b.config.DriverName, host); err != nil {
						b.logger.Errorf("fail to open DB: %s - %s", host, err.Error())
						continue
					} else {
						dbs[key] = connect
						db = connect
					}
				}
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
				delay:  time.Duration(b.config.MaxInterval.Nanoseconds() / int64(len(b.config.Ip)) * int64((len(b.committer))-1)),
				db:     db,
				config: b.config,
				logger: b.logger.Position(ip),
			}
			b.committer = append(b.committer, c)
		}
		if len(b.committer) <= b.config.MinAliveConnection {
			return ErrLessConnection
		}
		lock.Unlock()
	}
	return nil
}

func (b *Insertion) Insert(s ...string) error {
	b.Lock()
	defer b.Unlock()
	if err := b.init(); err != nil {
		return err
	}
	for i, ss := range s {
		for !b.next().Insert(ss) {
			b.logger.Errorf("committer: %d already closed")
			if err := b.CleanDied(); err != nil {
				writer := OpenWriter(b.config.BackUpPath, b.config.BackUpFilePrefix+"_"+fmt.Sprintf("%d", rand.Int()), "dump", 0)
				b.logger.Infof("backup: save died msg in %s", writer.path)
				writer.Flush(s[i:]...)
				writer.Close()
				return err
			}
		}
	}
	return nil
}

func (b *Insertion) CleanDied() error {
	b.index = 0
	alive := []*commit{}
	for _, v := range b.committer {
		if v.Alive() {
			alive = append(alive, v)
		}
	}
	if len(alive) <= b.config.MinAliveConnection {
		return ErrLessConnection
	} else {
		b.committer = alive
		return nil
	}
}

func (b *Insertion) next() *commit {
	b.index = (b.index + 1) % len(b.committer)
	return b.committer[b.index]
}

func (b *Insertion) LoadBackUp(includeCache bool) {
	b.logger.Infof("walk backup %s start", b.config.BackUpPath)
	err := filepath.Walk(b.config.BackUpPath, func(filepath string, fi os.FileInfo, err error) error {
		if err != nil {
			b.logger.Tracef("walk backup: error %s", err.Error())
			return err
		}
		if start, end, cache :=
			strings.Index(fi.Name(), b.config.BackUpFilePrefix+"_"),
			strings.Index(fi.Name(), ".dump"),
			strings.Index(fi.Name(), ".cache");
			start != -1 && end != -1 || (includeCache && cache != -1) {
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
