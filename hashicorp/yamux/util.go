package yamux

import (
	"sync"
	"time"
)

var (
	timerPool = &sync.Pool{
		New: func() interface{} {
			timer := time.NewTimer(time.Hour * 1e6)
			timer.Stop()
			return timer
		},
	}
)

func asyncSendErr(ch chan error, err error) {
	if ch == nil {
		return
	}
	select {
	case ch <- err:
	default:
	}
}

func asyncNotify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func min(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}
