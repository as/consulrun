package consul

import (
	"sync"
	"time"
)

type SessionTimers struct {
	sync.RWMutex
	m map[string]*time.Timer
}

func NewSessionTimers() *SessionTimers {
	return &SessionTimers{m: make(map[string]*time.Timer)}
}

func (t *SessionTimers) Get(id string) *time.Timer {
	t.RLock()
	defer t.RUnlock()
	return t.m[id]
}

func (t *SessionTimers) Set(id string, tm *time.Timer) {
	t.Lock()
	defer t.Unlock()
	if tm == nil {
		delete(t.m, id)
	} else {
		t.m[id] = tm
	}
}

func (t *SessionTimers) Del(id string) {
	t.Set(id, nil)
}

func (t *SessionTimers) Len() int {
	t.RLock()
	defer t.RUnlock()
	return len(t.m)
}

func (t *SessionTimers) ResetOrCreate(id string, ttl time.Duration, afterFunc func()) {
	t.Lock()
	defer t.Unlock()

	if tm := t.m[id]; tm != nil {
		tm.Reset(ttl)
		return
	}
	t.m[id] = time.AfterFunc(ttl, afterFunc)
}

func (t *SessionTimers) Stop(id string) {
	t.Lock()
	defer t.Unlock()
	if tm := t.m[id]; tm != nil {
		tm.Stop()
		delete(t.m, id)
	}
}

func (t *SessionTimers) StopAll() {
	t.Lock()
	defer t.Unlock()
	for _, tm := range t.m {
		tm.Stop()
	}
	t.m = make(map[string]*time.Timer)
}
