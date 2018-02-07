package zysql

import (
	"time"
	"sync"
	"fmt"
	"github.com/zyfcn/zyLog"
	"bufio"
	"os"
	"path/filepath"
	"io"
	"strings"
	"github.com/satori/go.uuid"
	"database/sql"
	"math/rand"
)

func NewInsertion(logger *zylog.ZyLogger, config Config, sql string, parser func(s string) ([]interface{}, error)) *Insertion {
	defer zylog.CatchAndThrow()
	b := &Insertion{
		Config: config,
		sql:    sql,
		parser: parser,
		logger: logger.GetChild("zysql"),
	}
	return b
}

type Insertion struct {
	db  *sql.DB
	sql string
	parser func(s string) ([]interface{}, error)

	AfterCommit func()
	Throw       func(error)

	close      bool
	lastCommit time.Time
	logger     *zylog.ChildLogger

	cache  []string
	thread chan int
	timer  *time.Timer
	uuid   string
	backup *Writer

	Config
	stat Statistics
	sync.Mutex
}

func (b *Insertion) init() {
	if err := b.check(); err != nil {
		b.logger.Fatal(err)
	}
	if b.db == nil {
		b.db = b.newConn()
	}
	if b.cache == nil {
		b.cache = []string{}
	}
	if len(b.cache) == 0 {
		uid, _ := uuid.NewV4()
		b.uuid = uid.String()
	}
	if b.thread == nil {
		b.thread = make(chan int, b.config.MaxThreads)
	}
}

func (b *Insertion) newConn() *sql.DB {
	storeLock.Lock()
	defer storeLock.Unlock()
	key := fmt.Sprintf("%s-%s:%d-%s", b.DriverName, b.Ip, b.Port, b.DataBase)
	if v, b := dbs[key]; b {
		return v
	}
	var try int
	for {
		var ip string
		switch try {
		case 0:
			ip = "localhost"
		case 1:
			ip = b.config.Ip[rand.Intn(len(b.config.Ip)-1)]
		default:
			if try > len(b.config.Ip)+2 {
				b.logger.Fatal("unable to reach any database")
				return nil
			} else {
				ip = b.config.Ip[try-2]
			}
		}
		host := b.config.DataSourceGenerator(ip, b.config.Port, b.DataBase)
		if connect, err := sql.Open(b.config.DriverName, host); err != nil {
			b.logger.Debugf("fail to open DB at %s(%s)", ip, err.Error())
		} else {
			if err = connect.Ping(); err == nil {
				dbs[key] = connect
				b.logger.Infof("connect %s success", host)
				return connect
			} else {
				b.logger.Debugf("fail to ping %s:%d(%s)", ip, b.config.Port, err.Error())
			}
		}
		try ++
	}
}

func (b *Insertion) SetSql(sql string, parser func(s string) ([]interface{}, error)) *Insertion {
	b.Lock()
	defer b.Unlock()
	b.sql = sql
	b.parser = parser
	return b
}

func (t *Insertion) Close() {
	t.Lock()
	defer t.Unlock()
	t.close = true
	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
}

func (b *Insertion) commitWithRetry(uuid string, cacheBackUp *Writer, retry int, cache []string, sql string, parser func(s string) ([]interface{}, error)) {
	defer func() {
		if err := recover(); err != nil {
			b.logger.Error(err)
			retry ++
			if retry < int(b.config.MaxRetry) {
				b.commitWithRetry(uuid, cacheBackUp, retry, cache, sql, parser)
			} else {
				<-b.thread
				b.logger.Tracef("%s commit: failed %d times, dropping", uuid, retry)
				if b.config.BackUpLevel == DiskBackUp {
					b.logger.Tracef("%s commit: backup on disk", uuid, retry)
					b.diskBackUp(uuid, cache)
				}
			}
		} else if b.config.BackUpLevel == CacheBackUp && cacheBackUp != nil {
			b.logger.Tracef("%s cache clear backup %s", uuid, cacheBackUp.path)
			cacheBackUp.Clear()
		}
	}()
	b.logger.Tracef("%s commit: start %dst time", uuid, retry)
	tx, err := b.db.Begin()
	if err != nil {
		panic(fmt.Sprintf("%s commit: Begin() failed: %s", uuid, err.Error()))
	}
	stmt, err := tx.Prepare(sql)
	if err != nil {
		panic(fmt.Sprintf("%s commit: Prepare() failed: %s", uuid, err.Error()))
	}

	for i, s := range cache {
		if data, err := parser(s); err == nil {
			if _, err2 := stmt.Exec(data...); err2 != nil {
				panic(fmt.Sprintf("%s commit: Exec() failed at line %d: %s", uuid, i+1, err2.Error()))
			}
		} else {
			panic(fmt.Sprintf("%s commit: parser() failed at line %d: %s", uuid, i+1, err.Error()))
		}
	}
	if err := tx.Commit(); err != nil {
		panic(fmt.Sprintf("%s commit: Commit() failed: %s", uuid, err.Error()))
	}
	if b.AfterCommit != nil {
		go b.AfterCommit()
	}
	b.logger.Tracef("%s commit: success for the %dst time, current threads %d", uuid, retry, len(b.thread))
	<-b.thread
}

func (b *Insertion) Commit() {
	b.Lock()
	defer b.Unlock()
	b.init()
	if b.cache != nil && len(b.cache) > 0 {
		cache := make([]string, len(b.cache))
		copy(cache, b.cache)
		b.cache = []string{}
		b.lastCommit = time.Now()
		b.thread <- 1
		b.logger.Tracef("%s commit: submit %d, current threads %d", b.uuid, len(cache), len(b.thread))
		go b.commitWithRetry(b.uuid, b.backup, 1, cache, b.sql, b.parser)
	} else {
		b.logger.Tracef("%s commit: empty", b.uuid)
	}

	//reset backup and uuid
	uid, _ := uuid.NewV4()
	b.uuid = uid.String()
	b.backup = nil
}

func (b *Insertion) Insert(s ...string) {
	b.Lock()
	defer b.Unlock()
	if len(s) == 0 {
		return
	}
	b.init()
	if b.lastCommit.IsZero() {
		b.lastCommit = time.Now()
	}
	if b.timer == nil && b.config.MaxInterval > 0 {
		timer := time.NewTimer(b.config.MaxInterval)
		go func() {
			defer b.logger.Info("timer exit")
			time.Sleep(b.config.MaxInterval)
			for !b.close {
				 <-timer.C
				b.logger.Tracef("%s commit: reach MaxInterval", b.uuid)
				b.Commit()
			}
		}()
		b.timer = timer
		b.logger.Infof("%s commit: init timer", b.uuid)
	} else if len(b.cache) == 0 {
		//reset timer
		b.timer.Stop()
		success := !b.timer.Reset(b.config.MaxInterval)
		b.logger.Tracef("%s commit: reset timer success is %t", b.uuid, success)
	}
	b.cacheBackUp(s...)
	b.cache = append(b.cache, s...)
	if len(b.cache) > int(b.config.MaxBatchSize) {
		b.logger.Tracef("%s commit: reach MaxBatchSize", b.uuid)
		//stop timer
		if b.timer.Stop() {
			b.Commit()
		}
	}
}

func (b *Insertion) LoadBackUp() {
	b.init()
	if err := b.check(); err != nil {
		b.logger.Fatal(err)
	}
	b.logger.Infof("walk backup %s start", b.config.BackUpPath)
	err := filepath.Walk(b.config.BackUpPath, func(filepath string, fi os.FileInfo, err error) error {
		if err != nil {
			b.logger.Tracef("walk backup: error %s", err.Error())
			return err
		}
		if start, end := strings.Index(fi.Name(), b.config.BackUpFilePrefix+"_"), strings.Index(fi.Name(), ".dump"); start != -1 && end != -1 {
			uid := fi.Name()[start+len(b.config.BackUpFilePrefix+"_"):end]
			b.logger.Tracef("backup: find %s, start", filepath)
			file, _ := os.Open(filepath)
			buf := bufio.NewReader(file)
			lines := []string{}
			for {
				line, _, err := buf.ReadLine()
				if err == io.EOF {
					break
				}
				lines = append(lines, string(line))
			}
			if len(lines) > 0 {
				b.logger.Tracef("%s backup: read %d messages", uid, len(lines))
				b.thread <- 1
				b.commitWithRetry(uid, b.backup, 1, lines, b.sql, b.parser)
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

func (b *Insertion) openWriter(uuid string, no int) *Writer {
	var suffix string
	if no > 0 {
		suffix = fmt.Sprintf("(%d)", no)
	}
	path := b.config.BackUpPath + "/" + b.config.BackUpFilePrefix + "_" + uuid + suffix + ".dump"
	var file *os.File
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		return b.openWriter(uuid, no+1)
	} else {
		absPath, _ := filepath.Abs(path)
		file, _ = os.OpenFile(absPath, os.O_RDWR|os.O_CREATE, 0777)
		return &Writer{
			path:   absPath,
			file:   file,
			writer: bufio.NewWriter(file),
		}
	}
}

func (b *Insertion) cacheBackUp(s ...string) {
	if b.config.BackUpLevel == CacheBackUp {
		if b.backup == nil {
			writer := b.openWriter(b.uuid, 0)
			b.backup = writer
			b.logger.Tracef("%s cache open backup %s", b.uuid, b.backup.path)
		}
		b.backup.Flush(s...)
	}
}

func (b *Insertion) diskBackUp(uuid string, cache []string) {
	go func() {
		if b.config.BackUpLevel > DisableBackUp && len(cache) != 0 {
			writer := b.openWriter(uuid, 0)
			err := <-writer.Flush(cache...)
			writer.Close()
			if err != nil {
				b.logger.Errorf("%s disk backup error : %s", uuid, err.Error())
			} else {
				b.logger.Infof("%s disk backup %d messages into %s", uuid, len(cache), writer.path)
			}
		}
	}()
}
