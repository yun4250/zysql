package zysql

import (
	"time"
	"database/sql"
	"fmt"
	"sync"
	"github.com/zyfcn/zyLog"
	"github.com/satori/go.uuid"
)

type commit struct {
	db    *sql.DB
	close bool

	cache  []string
	thread chan int
	timer  *time.Timer
	uuid   string
	backup *Writer

	config *Config
	logger *zylog.ChildLogger
	stat   Statistics
	sync.Mutex
}

func (c *commit) init() {
	if c.cache == nil {
		c.cache = []string{}
		uid, _ := uuid.NewV4()
		c.uuid = uid.String()
		c.thread = make(chan int, c.config.MaxThreads)
	}
}

func (c *commit) commitWithRetry(uuid string, cacheBackUp *Writer, retry int, cache []string, sql string, parser func(s string) ([]interface{}, error)) {
	defer func() {
		if err := recover(); err != nil {
			c.logger.Error(err)
			retry ++
			if retry < int(c.config.MaxRetry) {
				c.commitWithRetry(uuid, cacheBackUp, retry, cache, sql, parser)
			} else {
				<-c.thread
				c.logger.Tracef("%s commit: failed %d times, dropping", uuid, retry)
				if c.config.BackUpLevel == DiskBackUp {
					writer := OpenWriter(c.config.BackUpPath, c.config.BackUpFilePrefix+"_"+c.uuid, 0)
					c.logger.Tracef("%s commit: backup in %s", uuid, writer.path)
					writer.Flush(cache...)
					writer.Close()
				}
			}
		} else if c.config.BackUpLevel == CacheBackUp && cacheBackUp != nil {
			c.logger.Tracef("%s cache clear backup %s", uuid, cacheBackUp.path)
			cacheBackUp.Clear()
		}
	}()

	c.logger.Tracef("%s commit: start %dst time", uuid, retry)
	tx, err := c.db.Begin()
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
	c.logger.Tracef("%s commit: success for the %dst time, current threads %d", uuid, retry, len(c.thread))
	<-c.thread
}

func (c *commit) Commit() {
	c.Lock()
	defer c.Unlock()
	c.init()
	if len(c.cache) > 0 {
		cache := make([]string, len(c.cache))
		copy(cache, c.cache)
		c.cache = []string{}
		c.thread <- 1
		c.logger.Tracef("%s commit: submit %d, current threads %d", c.uuid, len(cache), len(c.thread))
		go c.commitWithRetry(c.uuid, c.backup, 1, cache, c.config.Sql, c.config.Parser)
	} else {
		c.logger.Tracef("%s commit: empty", c.uuid)
	}

	//reset backup and uuid
	uid, _ := uuid.NewV4()
	c.uuid = uid.String()
	c.backup = nil
}

func (c *commit) Insert(s ...string) {
	c.Lock()
	defer c.Unlock()
	c.init()
	if len(s) == 0 || c.close {
		return
	}
	size := len(c.cache)
	if c.timer == nil && c.config.MaxInterval > 0 {
		timer := time.NewTimer(c.config.MaxInterval)
		go func() {
			defer c.logger.Info("timer exit")
			time.Sleep(c.config.MaxInterval)
			for !c.close {
				<-timer.C
				c.logger.Tracef("%s commit: reach MaxInterval", c.uuid)
				c.Commit()
			}
		}()
		c.timer = timer
		c.logger.Infof("%s commit: init timer", c.uuid)
	} else if size == 0 {
		//reset timer
		c.timer.Stop()
		success := !c.timer.Reset(c.config.MaxInterval)
		c.logger.Tracef("%s commit: reset timer success is %t", c.uuid, success)
	}

	if c.config.BackUpLevel == CacheBackUp {
		if c.backup == nil {
			writer := OpenWriter(c.config.BackUpPath, c.config.BackUpFilePrefix+"_"+c.uuid, 0)
			c.backup = writer
			c.logger.Tracef("%s cache open backup %s", c.uuid, c.backup.path)
		}
		c.backup.Flush(s...)
	}
	c.cache = append(c.cache, s...)

	if uint(size) > c.config.MaxBatchSize {
		c.logger.Tracef("%s commit: reach MaxBatchSize", c.uuid)
		//stop timer
		if c.timer.Stop() {
			c.Commit()
		}
	}
}

func (c *commit) Close() {
	c.Lock()
	defer c.Unlock()
	if c.timer != nil {
		if c.timer.Stop() {
			c.Commit()
		}
	}
	c.close = true
}
