package zysql

import (
	"sync/atomic"
	"time"
	"sync"
)

type Statistics struct {
	begin           time.Time
	last 			time.Time
	timer           *time.Ticker
	num             int64
	total           int64
	rateLastSeconds int64
	rateLastMinutes int64
	rateLastHour    int64
	history         map[int64]int64
	sync.Mutex
}

func (s *Statistics) checkAndStart() {
	s.Lock()
	defer s.Unlock()
	if s.timer == nil {
		timer := time.NewTicker(time.Second)
		go func() {
			for {
				select {
				case <-timer.C:
					//r.RatePerSecond = float64(s.num)
					atomic.SwapInt64(&s.num, 0)
				}
			}
		}()
	}
}

func (s *Statistics) Incr(n int64) {
	s.checkAndStart()
	atomic.AddInt64(&s.num, n)
	atomic.AddInt64(&s.total, n)
}

func (s *Statistics) Incr1() {
	s.checkAndStart()
	atomic.AddInt64(&s.num, 1)
	atomic.AddInt64(&s.total, 1)
}

func (s *Statistics) Stop() {

}
