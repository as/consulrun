package lru

import (
	"sync"

	"github.com/hashicorp/golang-lru/simplelru"
)

type ARCCache struct {
	size int // Size is the total capacity of the cache
	p    int // P is the dynamic preference towards T1 or T2

	t1 *simplelru.LRU // T1 is the LRU for recently accessed items
	b1 *simplelru.LRU // B1 is the LRU for evictions from t1

	t2 *simplelru.LRU // T2 is the LRU for frequently accessed items
	b2 *simplelru.LRU // B2 is the LRU for evictions from t2

	lock sync.RWMutex
}

func NewARC(size int) (*ARCCache, error) {

	b1, err := simplelru.NewLRU(size, nil)
	if err != nil {
		return nil, err
	}
	b2, err := simplelru.NewLRU(size, nil)
	if err != nil {
		return nil, err
	}
	t1, err := simplelru.NewLRU(size, nil)
	if err != nil {
		return nil, err
	}
	t2, err := simplelru.NewLRU(size, nil)
	if err != nil {
		return nil, err
	}

	c := &ARCCache{
		size: size,
		p:    0,
		t1:   t1,
		b1:   b1,
		t2:   t2,
		b2:   b2,
	}
	return c, nil
}

func (c *ARCCache) Get(key interface{}) (interface{}, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if val, ok := c.t1.Peek(key); ok {
		c.t1.Remove(key)
		c.t2.Add(key, val)
		return val, ok
	}

	if val, ok := c.t2.Get(key); ok {
		return val, ok
	}

	return nil, false
}

func (c *ARCCache) Add(key, value interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.t1.Contains(key) {
		c.t1.Remove(key)
		c.t2.Add(key, value)
		return
	}

	if c.t2.Contains(key) {
		c.t2.Add(key, value)
		return
	}

	if c.b1.Contains(key) {

		delta := 1
		b1Len := c.b1.Len()
		b2Len := c.b2.Len()
		if b2Len > b1Len {
			delta = b2Len / b1Len
		}
		if c.p+delta >= c.size {
			c.p = c.size
		} else {
			c.p += delta
		}

		if c.t1.Len()+c.t2.Len() >= c.size {
			c.replace(false)
		}

		c.b1.Remove(key)

		c.t2.Add(key, value)
		return
	}

	if c.b2.Contains(key) {

		delta := 1
		b1Len := c.b1.Len()
		b2Len := c.b2.Len()
		if b1Len > b2Len {
			delta = b1Len / b2Len
		}
		if delta >= c.p {
			c.p = 0
		} else {
			c.p -= delta
		}

		if c.t1.Len()+c.t2.Len() >= c.size {
			c.replace(true)
		}

		c.b2.Remove(key)

		c.t2.Add(key, value)
		return
	}

	if c.t1.Len()+c.t2.Len() >= c.size {
		c.replace(false)
	}

	if c.b1.Len() > c.size-c.p {
		c.b1.RemoveOldest()
	}
	if c.b2.Len() > c.p {
		c.b2.RemoveOldest()
	}

	c.t1.Add(key, value)
	return
}

func (c *ARCCache) replace(b2ContainsKey bool) {
	t1Len := c.t1.Len()
	if t1Len > 0 && (t1Len > c.p || (t1Len == c.p && b2ContainsKey)) {
		k, _, ok := c.t1.RemoveOldest()
		if ok {
			c.b1.Add(k, nil)
		}
	} else {
		k, _, ok := c.t2.RemoveOldest()
		if ok {
			c.b2.Add(k, nil)
		}
	}
}

func (c *ARCCache) Len() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.t1.Len() + c.t2.Len()
}

func (c *ARCCache) Keys() []interface{} {
	c.lock.RLock()
	defer c.lock.RUnlock()
	k1 := c.t1.Keys()
	k2 := c.t2.Keys()
	return append(k1, k2...)
}

func (c *ARCCache) Remove(key interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.t1.Remove(key) {
		return
	}
	if c.t2.Remove(key) {
		return
	}
	if c.b1.Remove(key) {
		return
	}
	if c.b2.Remove(key) {
		return
	}
}

func (c *ARCCache) Purge() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.t1.Purge()
	c.t2.Purge()
	c.b1.Purge()
	c.b2.Purge()
}

func (c *ARCCache) Contains(key interface{}) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.t1.Contains(key) || c.t2.Contains(key)
}

func (c *ARCCache) Peek(key interface{}) (interface{}, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if val, ok := c.t1.Peek(key); ok {
		return val, ok
	}
	return c.t2.Peek(key)
}
