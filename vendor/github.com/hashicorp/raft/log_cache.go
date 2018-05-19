package raft

import (
	"fmt"
	"sync"
)

type LogCache struct {
	store LogStore

	cache []*Log
	l     sync.RWMutex
}

func NewLogCache(capacity int, store LogStore) (*LogCache, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("capacity must be positive")
	}
	c := &LogCache{
		store: store,
		cache: make([]*Log, capacity),
	}
	return c, nil
}

func (c *LogCache) GetLog(idx uint64, log *Log) error {

	c.l.RLock()
	cached := c.cache[idx%uint64(len(c.cache))]
	c.l.RUnlock()

	if cached != nil && cached.Index == idx {
		*log = *cached
		return nil
	}

	return c.store.GetLog(idx, log)
}

func (c *LogCache) StoreLog(log *Log) error {
	return c.StoreLogs([]*Log{log})
}

func (c *LogCache) StoreLogs(logs []*Log) error {

	c.l.Lock()
	for _, l := range logs {
		c.cache[l.Index%uint64(len(c.cache))] = l
	}
	c.l.Unlock()

	return c.store.StoreLogs(logs)
}

func (c *LogCache) FirstIndex() (uint64, error) {
	return c.store.FirstIndex()
}

func (c *LogCache) LastIndex() (uint64, error) {
	return c.store.LastIndex()
}

func (c *LogCache) DeleteRange(min, max uint64) error {

	c.l.Lock()
	c.cache = make([]*Log, len(c.cache))
	c.l.Unlock()

	return c.store.DeleteRange(min, max)
}
