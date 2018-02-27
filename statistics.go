package zysql

import (
	"sync/atomic"
	"time"
	"sync"
	"fmt"
)

type Statistics struct {
	begin           time.Time
	rateLastSeconds int64
	total           int64

	lastCommit  time.Time
	totalCommit int64

	timer *time.Ticker
	sync.Mutex
}

func (s *Statistics) insert(i int) {
	if s.begin.IsZero() {
		s.start()
	}
	atomic.AddInt64(&s.rateLastSeconds, int64(i))
}

func (s *Statistics) commit() {
	s.lastCommit = time.Now()
	atomic.AddInt64(&s.totalCommit, 1)
}

func (s *Statistics) start() {
	s.Lock()
	defer s.Unlock()
	s.begin = time.Now()
	if s.timer == nil {
		timer := time.NewTicker(time.Second)
		go func() {
			for {
				select {
				case <-timer.C:
					num := atomic.SwapInt64(&s.rateLastSeconds, 0)
					atomic.AddInt64(&s.total, num)
				}
			}
		}()
	}
}

func (s *Statistics) Health() map[string]string {
	m := make(map[string]string)
	m["begin"] = s.begin.String()
	m["lastCommit"] = s.lastCommit.Format("2006-01-02 15-04-05.000")
	m["totalCommit"] = fmt.Sprint(s.totalCommit)
	m["totalInsert"] = fmt.Sprint(s.total)
	m["lastSecondInsert"] = fmt.Sprint(s.rateLastSeconds)
	return m
}
