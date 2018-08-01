package state

import (
	"fmt"
	"sync"
	"time"
)

//
//
type TombstoneGC struct {
	ttl time.Duration

	granularity time.Duration

	enabled bool

	expires map[time.Time]*expireInterval

	expireCh chan uint64

	sync.Mutex
}

type expireInterval struct {
	maxIndex uint64

	timer *time.Timer
}

func NewTombstoneGC(ttl, granularity time.Duration) (*TombstoneGC, error) {

	if ttl <= 0 || granularity <= 0 {
		return nil, fmt.Errorf("Tombstone TTL and granularity must be positive")
	}

	t := &TombstoneGC{
		ttl:         ttl,
		granularity: granularity,
		expires:     make(map[time.Time]*expireInterval),
		expireCh:    make(chan uint64, 1),
	}
	return t, nil
}

func (t *TombstoneGC) ExpireCh() <-chan uint64 {
	return t.expireCh
}

func (t *TombstoneGC) SetEnabled(enabled bool) {
	t.Lock()
	defer t.Unlock()
	if enabled == t.enabled {
		return
	}

	if !enabled {
		for _, exp := range t.expires {
			exp.timer.Stop()
		}
		t.expires = make(map[time.Time]*expireInterval)
	}

	t.enabled = enabled
}

func (t *TombstoneGC) Hint(index uint64) {
	expires := t.nextExpires()

	t.Lock()
	defer t.Unlock()
	if !t.enabled {
		return
	}

	exp, ok := t.expires[expires]
	if ok {
		if index > exp.maxIndex {
			exp.maxIndex = index
		}
		return
	}

	t.expires[expires] = &expireInterval{
		maxIndex: index,
		timer: time.AfterFunc(expires.Sub(time.Now()), func() {
			t.expireTime(expires)
		}),
	}
}

func (t *TombstoneGC) PendingExpiration() bool {
	t.Lock()
	defer t.Unlock()

	return len(t.expires) > 0
}

func (t *TombstoneGC) nextExpires() time.Time {

	expires := time.Now().Add(t.ttl).Round(0)
	remain := expires.UnixNano() % int64(t.granularity)
	adj := expires.Add(t.granularity - time.Duration(remain))
	return adj
}

func (t *TombstoneGC) purgeBin(expires time.Time) uint64 {
	t.Lock()
	defer t.Unlock()

	exp, ok := t.expires[expires]
	if !ok {
		return 0
	}
	delete(t.expires, expires)
	return exp.maxIndex
}

func (t *TombstoneGC) expireTime(expires time.Time) {

	if index := t.purgeBin(expires); index > 0 {
		t.expireCh <- index
	}
}
