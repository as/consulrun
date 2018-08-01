package serf

import (
	"sync/atomic"
)

type LamportClock struct {
	counter uint64
}

type LamportTime uint64

func (l *LamportClock) Time() LamportTime {
	return LamportTime(atomic.LoadUint64(&l.counter))
}

func (l *LamportClock) Increment() LamportTime {
	return LamportTime(atomic.AddUint64(&l.counter, 1))
}

func (l *LamportClock) Witness(v LamportTime) {
WITNESS:

	cur := atomic.LoadUint64(&l.counter)
	other := uint64(v)
	if other < cur {
		return
	}

	if !atomic.CompareAndSwapUint64(&l.counter, cur, other+1) {

		goto WITNESS
	}
}
