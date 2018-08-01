package state

import (
	"sync"
	"time"
)

type Delay struct {
	delay map[string]time.Time

	lock sync.RWMutex
}

func NewDelay() *Delay {
	return &Delay{delay: make(map[string]time.Time)}
}

func (d *Delay) GetExpiration(key string) time.Time {
	d.lock.RLock()
	expires := d.delay[key]
	d.lock.RUnlock()
	return expires
}

func (d *Delay) SetExpiration(key string, now time.Time, delay time.Duration) {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.delay[key] = now.Add(delay)
	time.AfterFunc(delay, func() {
		d.lock.Lock()
		delete(d.delay, key)
		d.lock.Unlock()
	})
}
