package zysql

import (
	"time"
	"container/list"
	"database/sql"
	"fmt"
	"sync"
	"github.com/zyfcn/zyLog"
	"github.com/satori/go.uuid"
	"strings"
	"database/sql/driver"
)

type commit struct {
	father     *Insertion
	ip         string
	driver     string
	dataSource string
	close      bool

	cache  *list.List
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
		c.cache = list.New()
		uid, _ := uuid.NewV4()
		c.uuid = uid.String()
		c.thread = make(chan string, c.config.MaxThreads)
	}
}

func (c *commit) Ping() error {
	if db, err := GetDb(c.driver, c.dataSource); err != nil {
		return err
	} else {
		return db.Ping()
	}
}

func (c *commit) commitWithRetry(uuid string, cacheBackUp *Writer, retry int, cache *list.List, insertSql string, parser func(s string) ([]interface{}, error)) {
	var (
		position string
		conn     *sql.DB
		err      error
		tx       *sql.Tx
		stmt     *sql.Stmt
		data     []interface{}
		start    = time.Now()
	)
	defer func() {
		err := recover()
		if c.config.CommitCallBack != nil {
			c.config.CommitCallBack(c.ip, c.dataSource, uuid, cache.Len(), retry, err)
		}
		if stmt != nil {
			stmt.Close()
		}
		if err != nil {
			c.logger.Errorf("%s commit: %s failed: %s", uuid, position, err)
			retry ++
			if retry < int(c.config.MaxRetry) {
				c.commitWithRetry(uuid, cacheBackUp, retry, cache, insertSql, parser)
			} else {
				<-c.thread
				c.logger.Errorf("%s commit: failed %d times, dropping", uuid, retry)
				if c.config.BackUpLevel == DiskBackUp {
					writer := OpenWriter(c.config.BackUpPath, c.config.BackUpFilePrefix+"_"+c.uuid, "dump", 0)
					c.logger.Infof("%s commit: backup in %s", uuid, writer.path)
					writer.FlushList(cache)
					writer.Close()
				} else if c.config.BackUpLevel == CacheBackUp {
					cacheBackUp.Rename(strings.Replace(cacheBackUp.path, ".cache", ".dump", -1))
				}
				if err == driver.ErrBadConn {
					c.Close()
				}
			}
		} else {
			c.logger.Tracef("%s commit: %dst time success(%s), current threads %d", uuid, retry, time.Since(start).String(), len(c.thread))
			<-c.thread
			c.stat.commit()
			if c.config.BackUpLevel == CacheBackUp {
				c.logger.Tracef("%s cache clear backup %s", uuid, cacheBackUp.path)
				cacheBackUp.Clear()
			}
		}
	}()
	c.logger.Tracef("%s commit: %dst time start", uuid, retry)
	conn, err = GetDb(c.driver, c.dataSource)
	position = "GetDb()"
	if err != nil {
		panic(err)
	}
	tx, err = conn.Begin()
	position = "Begin()"
	if err != nil {
		panic(err)
	}
	stmt, err = tx.Prepare(insertSql)
	position = "Prepare()"
	if err != nil {
		panic(err)
	}
	e := cache.Front()
	for i := 0; e != nil; i++ {
		position = fmt.Sprintf("parser() at line %d", i)
		if data, err = parser(e.Value.(string)); err != nil {
			panic(err)
		} else if _, err = stmt.Exec(data...); err != nil {
			position = fmt.Sprintf("Exec() at line %d", i)
			panic(err)
		}
		e = e.Next()
	}
	position = "Commit()"
	if err = tx.Commit(); err != nil {
		panic(err)
	}
}

func (c *commit) Commit() {
	c.Lock()
	defer c.Unlock()
	c.commit()
}

func (c *commit) commit() {
	c.father.next()
	c.init()
	if c.cache.Len() > 0 {
		c.logger.Debugf("%s commit: submit %d, current threads %d", c.uuid, c.cache.Len(), len(c.thread))
		go c.commitWithRetry(c.uuid, c.backup, 1, c.cache, c.config.Sql, c.config.Parser)
		c.cache = list.New()
		c.thread <- c.uuid
	} else {
		c.logger.Tracef("%s commit: empty", c.uuid)
	}

	//reset backup and uuid
	uid, _ := uuid.NewV4()
	c.uuid = uid.String()
	c.backup = nil
}

func (c *commit) Insert(s string) bool {
	c.Lock()
	defer c.Unlock()
	c.init()
	if c.close {
		if c.backup == nil {
			writer := OpenWriter(c.config.BackUpPath, c.config.BackUpFilePrefix+"_"+c.uuid, "cache", 0)
			c.backup = writer
			c.logger.Tracef("insert closed, write to %s", c.backup.path)
		}
		c.backup.Flush(s)
		return false
	}
	if len(s) == 0 {
		return true
	}
	if c.timer == nil && c.config.MaxInterval > 0 {
		c.timer = time.NewTimer(c.config.MaxInterval)
		go func() {
			defer c.logger.Tracef("timer exit")
			for !c.close {
				<-c.timer.C
				c.logger.Tracef("%s commit: reach MaxInterval", c.uuid)
				c.Commit()
			}
		}()
		c.logger.Infof("%s commit: init timer", c.uuid)
	} else if c.cache.Len() == 0 {
		//reset timer
		c.timer.Stop()
		success := !c.timer.Reset(c.config.MaxInterval)
		c.logger.Tracef("%s commit: reset timer success is %t", c.uuid, success)
	}

	if c.config.BackUpLevel == CacheBackUp {
		if c.backup == nil {
			c.backup = OpenWriter(c.config.BackUpPath, c.config.BackUpFilePrefix+"_"+c.uuid, "cache", 0)
			c.logger.Tracef("%s cache open backup %s", c.uuid, c.backup.path)
		}
		c.backup.Flush(s)
	}
	c.cache.PushBack(s)

	if uint(c.cache.Len()) > c.config.MaxBatchSize {
		c.logger.Tracef("%s commit: reach MaxBatchSize", c.uuid)
		//stop timer
		if c.timer.Stop() {
			c.commit()
		}
	}
	c.stat.insert(1)
	return true
}

func (c *commit) Alive() bool {
	c.Lock()
	defer c.Unlock()
	return !c.close
}

func (c *commit) Close() int {
	c.logger.Tracef("stopping")
	c.Lock()
	c.close = true
	for len(c.thread) != 0 {
		c.logger.Infof("stopped, %d commit left", len(c.thread))
		time.Sleep(time.Second)
	}
	if c.timer != nil {
		if c.timer.Stop() {
			c.timer.Reset(1)
		}
	}
	c.Unlock()
	c.logger.Infof("stopped\n" + c.stat.Health())
	return 1
}
