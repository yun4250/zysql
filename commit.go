package zysql

import (
	"time"
	"database/sql"
	"fmt"
	"sync"
	"github.com/zyfcn/zyLog"
	"github.com/satori/go.uuid"
	"strings"
	"database/sql/driver"
)

type commit struct {
	delay time.Duration
	db    *sql.DB
	close bool

	cache  []string
	thread chan string
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
		c.thread = make(chan string, c.config.MaxThreads)
	}
}

func (c *commit) commitWithRetry(uuid string, cacheBackUp *Writer, retry int, cache []string, insertSql string, parser func(s string) ([]interface{}, error)) {
	var position string
	defer func() {
		if err := recover(); err != nil {
			c.logger.Errorf("%s commit: %s failed: %s", uuid, position, err)
			retry ++
			if retry < int(c.config.MaxRetry) {
				c.commitWithRetry(uuid, cacheBackUp, retry, cache, insertSql, parser)
			} else {
				<-c.thread
				if c.config.CommitFailCallBack != nil {
					c.config.CommitFailCallBack(err)
				}
				c.logger.Errorf("%s commit: failed %d times, dropping", uuid, retry)
				if c.config.BackUpLevel == DiskBackUp {
					writer := OpenWriter(c.config.BackUpPath, c.config.BackUpFilePrefix+"_"+c.uuid, "dump", 0)
					c.logger.Infof("%s commit: backup in %s", uuid, writer.path)
					writer.Flush(cache...)
					writer.Close()
				} else if c.config.BackUpLevel == CacheBackUp {
					cacheBackUp.Rename(strings.Replace(cacheBackUp.path, ".cache", ".dump", -1))
				}
				if err == driver.ErrBadConn {
					c.Close()
				}
			}
		} else if c.config.BackUpLevel == CacheBackUp {
			c.logger.Tracef("%s cache clear backup %s", uuid, cacheBackUp.path)
			cacheBackUp.Clear()
		}
	}()
	start := time.Now()
	c.logger.Tracef("%s commit: %dst time start", uuid, retry)
	tx, err := c.db.Begin()
	if err != nil {
		position = "Begin()"
		panic(err)
	}

	stmt, err := tx.Prepare(insertSql)
	if err != nil {
		position = "Prepare()"
		panic(err)
	}

	for i, s := range cache {
		if data, err := parser(s); err == nil {
			if _, err2 := stmt.Exec(data...); err2 != nil {
				position = fmt.Sprintf("Exec() at line %d", i+1)
				panic(err2)
			}
		} else {
			position = fmt.Sprintf("parser() at line %d", i+1)
			panic(err)
		}
	}
	if err := tx.Commit(); err != nil {
		position = "Commit()"
		panic(err)
	}
	c.logger.Tracef("%s commit: %dst time success(%s), current threads %d", uuid, retry, time.Since(start).String(), len(c.thread))
	<-c.thread
}

func (c *commit) Commit() {
	c.Lock()
	defer c.Unlock()
	c.commit()
}

func (c *commit) commit() {
	c.init()
	if len(c.cache) > 0 {
		cache := make([]string, len(c.cache))
		copy(cache, c.cache)
		c.cache = []string{}
		c.thread <- c.uuid
		c.logger.Debugf("%s commit: submit %d, current threads %d", c.uuid, len(cache), len(c.thread))
		go c.commitWithRetry(c.uuid, c.backup, 1, cache, c.config.Sql, c.config.Parser)
	} else {
		c.logger.Tracef("%s commit: empty", c.uuid)
	}

	//reset backup and uuid
	uid, _ := uuid.NewV4()
	c.uuid = uid.String()
	c.backup = nil
}

func (c *commit) Insert(s ...string) bool {
	if c.close {
		return false
	}
	c.Lock()
	defer c.Unlock()
	c.init()
	if len(s) == 0 {
		return true
	}
	size := len(c.cache)
	if c.timer == nil && c.config.MaxInterval > 0 {
		c.timer = time.NewTimer(c.config.MaxInterval)
		go func() {
			defer c.logger.Info("timer exit")
			time.Sleep(c.delay)
			for !c.close {
				select {
				case <-c.timer.C:
					c.logger.Tracef("%s commit: reach MaxInterval", c.uuid)
					c.Commit()
				default:
				}
			}
		}()
		c.logger.Infof("%s commit: init timer", c.uuid)
	} else if size == 0 {
		//reset timer
		c.timer.Stop()
		success := !c.timer.Reset(c.config.MaxInterval)
		c.logger.Tracef("%s commit: reset timer success is %t", c.uuid, success)
	}

	if c.config.BackUpLevel == CacheBackUp {
		if c.backup == nil {
			writer := OpenWriter(c.config.BackUpPath, c.config.BackUpFilePrefix+"_"+c.uuid, "cache", 0)
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
			c.commit()
		}
	}
	return true
}

func (c *commit) Alive() bool {
	c.Lock()
	defer c.Unlock()
	return !c.close
}

func (c *commit) Close() int {
	c.logger.Infof("stopping")
	c.close = true
	c.Lock()
	for len(c.thread) != 0 {
		c.logger.Infof("stopped, %d commit left", len(c.thread))
		time.Sleep(time.Second)
	}
	if c.timer != nil {
		if c.timer.Stop() {
			c.commit()
		}
	}
	c.Unlock()
	c.logger.Infof("stopped")
	return 1
}
