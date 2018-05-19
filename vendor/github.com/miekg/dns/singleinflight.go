// Copyright 2013 The Go Authors.  All rights reserved.

package dns

import "sync"
import "time"

type call struct {
	wg   sync.WaitGroup
	val  *Msg
	rtt  time.Duration
	err  error
	dups int
}

type singleflight struct {
	sync.Mutex                  // protects m
	m          map[string]*call // lazily initialized
}

func (g *singleflight) Do(key string, fn func() (*Msg, time.Duration, error)) (v *Msg, rtt time.Duration, err error, shared bool) {
	g.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		g.Unlock()
		c.wg.Wait()
		return c.val, c.rtt, c.err, true
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.Unlock()

	c.val, c.rtt, c.err = fn()
	c.wg.Done()

	g.Lock()
	delete(g.m, key)
	g.Unlock()

	return c.val, c.rtt, c.err, c.dups > 0
}
