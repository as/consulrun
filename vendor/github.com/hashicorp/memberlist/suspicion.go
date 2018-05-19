package memberlist

import (
	"math"
	"sync/atomic"
	"time"
)

type suspicion struct {
	n int32

	k int32

	min time.Duration

	max time.Duration

	start time.Time

	timer *time.Timer

	timeoutFn func()

	confirmations map[string]struct{}
}

func newSuspicion(from string, k int, min time.Duration, max time.Duration, fn func(int)) *suspicion {
	s := &suspicion{
		k:             int32(k),
		min:           min,
		max:           max,
		confirmations: make(map[string]struct{}),
	}

	s.confirmations[from] = struct{}{}

	s.timeoutFn = func() {
		fn(int(atomic.LoadInt32(&s.n)))
	}

	timeout := max
	if k < 1 {
		timeout = min
	}
	s.timer = time.AfterFunc(timeout, s.timeoutFn)

	s.start = time.Now()
	return s
}

func remainingSuspicionTime(n, k int32, elapsed time.Duration, min, max time.Duration) time.Duration {
	frac := math.Log(float64(n)+1.0) / math.Log(float64(k)+1.0)
	raw := max.Seconds() - frac*(max.Seconds()-min.Seconds())
	timeout := time.Duration(math.Floor(1000.0*raw)) * time.Millisecond
	if timeout < min {
		timeout = min
	}

	return timeout - elapsed
}

func (s *suspicion) Confirm(from string) bool {

	if atomic.LoadInt32(&s.n) >= s.k {
		return false
	}

	if _, ok := s.confirmations[from]; ok {
		return false
	}
	s.confirmations[from] = struct{}{}

	n := atomic.AddInt32(&s.n, 1)
	elapsed := time.Since(s.start)
	remaining := remainingSuspicionTime(n, s.k, elapsed, s.min, s.max)
	if s.timer.Stop() {
		if remaining > 0 {
			s.timer.Reset(remaining)
		} else {
			go s.timeoutFn()
		}
	}
	return true
}
