package lru

import (
	"fmt"
	"sync"

	"github.com/hashicorp/golang-lru/simplelru"
)

const (
	Default2QRecentRatio = 0.25

	Default2QGhostEntries = 0.50
)

type TwoQueueCache struct {
	size       int
	recentSize int

	recent      *simplelru.LRU
	frequent    *simplelru.LRU
	recentEvict *simplelru.LRU
	lock        sync.RWMutex
}

func New2Q(size int) (*TwoQueueCache, error) {
	return New2QParams(size, Default2QRecentRatio, Default2QGhostEntries)
}

func New2QParams(size int, recentRatio float64, ghostRatio float64) (*TwoQueueCache, error) {
	if size <= 0 {
		return nil, fmt.Errorf("invalid size")
	}
	if recentRatio < 0.0 || recentRatio > 1.0 {
		return nil, fmt.Errorf("invalid recent ratio")
	}
	if ghostRatio < 0.0 || ghostRatio > 1.0 {
		return nil, fmt.Errorf("invalid ghost ratio")
	}

	recentSize := int(float64(size) * recentRatio)
	evictSize := int(float64(size) * ghostRatio)

	recent, err := simplelru.NewLRU(size, nil)
	if err != nil {
		return nil, err
	}
	frequent, err := simplelru.NewLRU(size, nil)
	if err != nil {
		return nil, err
	}
	recentEvict, err := simplelru.NewLRU(evictSize, nil)
	if err != nil {
		return nil, err
	}

	c := &TwoQueueCache{
		size:        size,
		recentSize:  recentSize,
		recent:      recent,
		frequent:    frequent,
		recentEvict: recentEvict,
	}
	return c, nil
}

func (c *TwoQueueCache) Get(key interface{}) (interface{}, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if val, ok := c.frequent.Get(key); ok {
		return val, ok
	}

	if val, ok := c.recent.Peek(key); ok {
		c.recent.Remove(key)
		c.frequent.Add(key, val)
		return val, ok
	}

	return nil, false
}

func (c *TwoQueueCache) Add(key, value interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.frequent.Contains(key) {
		c.frequent.Add(key, value)
		return
	}

	if c.recent.Contains(key) {
		c.recent.Remove(key)
		c.frequent.Add(key, value)
		return
	}

	if c.recentEvict.Contains(key) {
		c.ensureSpace(true)
		c.recentEvict.Remove(key)
		c.frequent.Add(key, value)
		return
	}

	c.ensureSpace(false)
	c.recent.Add(key, value)
	return
}

func (c *TwoQueueCache) ensureSpace(recentEvict bool) {

	recentLen := c.recent.Len()
	freqLen := c.frequent.Len()
	if recentLen+freqLen < c.size {
		return
	}

	if recentLen > 0 && (recentLen > c.recentSize || (recentLen == c.recentSize && !recentEvict)) {
		k, _, _ := c.recent.RemoveOldest()
		c.recentEvict.Add(k, nil)
		return
	}

	c.frequent.RemoveOldest()
}

func (c *TwoQueueCache) Len() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.recent.Len() + c.frequent.Len()
}

func (c *TwoQueueCache) Keys() []interface{} {
	c.lock.RLock()
	defer c.lock.RUnlock()
	k1 := c.frequent.Keys()
	k2 := c.recent.Keys()
	return append(k1, k2...)
}

func (c *TwoQueueCache) Remove(key interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.frequent.Remove(key) {
		return
	}
	if c.recent.Remove(key) {
		return
	}
	if c.recentEvict.Remove(key) {
		return
	}
}

func (c *TwoQueueCache) Purge() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.recent.Purge()
	c.frequent.Purge()
	c.recentEvict.Purge()
}

func (c *TwoQueueCache) Contains(key interface{}) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.frequent.Contains(key) || c.recent.Contains(key)
}

func (c *TwoQueueCache) Peek(key interface{}) (interface{}, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if val, ok := c.frequent.Peek(key); ok {
		return val, ok
	}
	return c.recent.Peek(key)
}
