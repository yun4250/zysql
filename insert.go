package zysql

import (
	"sync"
	"github.com/zyfcn/zyLog"
	"bufio"
	"os"
	"path/filepath"
	"io"
	"strings"
	"syscall"
	"os/signal"
	"errors"
	"math/rand"
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
		for _, ip := range b.config.Ip {
			b.committer = append(b.committer, &commit{
				father:     b,
				driver:     b.config.DriverName,
				dataSource: b.config.DataSourceGenerator(ip, b.config.Port, b.config.DataBase),
				config:     b.config,
				ip:         ip,
				logger:     b.logger.Position(ip),
			})
		}
		b.index = rand.Intn(len(b.config.Ip))
		b.logger.Infof("Insertion init, starts with %s", b.config.Ip[b.index])
	}
	return nil
}

func (b *Insertion) Insert(s string) error {
	b.Lock()
	defer b.Unlock()
	if err := b.init(); err != nil {
		return err
	}
	if !b.committer[b.index].Insert(s) {
		b.logger.Errorf("committer: %d already closed")
		return b.cleanDied()
	}
	return nil
}

func (b *Insertion) cleanDied() error {
	b.logger.Info("clean started")
	b.index = 0
	alive := []*commit{}
	for _, v := range b.committer {
		if v.Alive() {
			alive = append(alive, v)
		} else if v.backup != nil {
			v.backup.Close()
		}
	}
	if len(alive) <= b.config.MinAliveConnection {
		b.logger.Errorf("alive client less than %d",b.config.MinAliveConnection)
		return ErrLessConnection
	} else {
		b.committer = alive
		return nil
	}
}

func (b *Insertion) next() {
	b.Lock()
	defer b.Unlock()
	b.index = (b.index + 1) % len(b.committer)
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
			start != -1 && end != -1 && (includeCache && cache != -1) {
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
