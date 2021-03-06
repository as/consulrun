// Copyright 2014 The Go Authors. All rights reserved.

package http2

import (
	"errors"
	"io"
	"sync"
)

type pipe struct {
	mu       sync.Mutex
	c        sync.Cond     // c.L lazily initialized to &p.mu
	b        pipeBuffer    // nil when done reading
	err      error         // read error once empty. non-nil means closed.
	breakErr error         // immediate read error (caller doesn't see rest of b)
	donec    chan struct{} // closed on error
	readFn   func()        // optional code to run in Read before error
}

type pipeBuffer interface {
	Len() int
	io.Writer
	io.Reader
}

func (p *pipe) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.b == nil {
		return 0
	}
	return p.b.Len()
}

func (p *pipe) Read(d []byte) (n int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.c.L == nil {
		p.c.L = &p.mu
	}
	for {
		if p.breakErr != nil {
			return 0, p.breakErr
		}
		if p.b != nil && p.b.Len() > 0 {
			return p.b.Read(d)
		}
		if p.err != nil {
			if p.readFn != nil {
				p.readFn()     // e.g. copy trailers
				p.readFn = nil // not sticky like p.err
			}
			p.b = nil
			return 0, p.err
		}
		p.c.Wait()
	}
}

var errClosedPipeWrite = errors.New("write on closed buffer")

func (p *pipe) Write(d []byte) (n int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.c.L == nil {
		p.c.L = &p.mu
	}
	defer p.c.Signal()
	if p.err != nil {
		return 0, errClosedPipeWrite
	}
	if p.breakErr != nil {
		return len(d), nil // discard when there is no reader
	}
	return p.b.Write(d)
}

//
func (p *pipe) CloseWithError(err error) { p.closeWithError(&p.err, err, nil) }

func (p *pipe) BreakWithError(err error) { p.closeWithError(&p.breakErr, err, nil) }

func (p *pipe) closeWithErrorAndCode(err error, fn func()) { p.closeWithError(&p.err, err, fn) }

func (p *pipe) closeWithError(dst *error, err error, fn func()) {
	if err == nil {
		panic("err must be non-nil")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.c.L == nil {
		p.c.L = &p.mu
	}
	defer p.c.Signal()
	if *dst != nil {

		return
	}
	p.readFn = fn
	if dst == &p.breakErr {
		p.b = nil
	}
	*dst = err
	p.closeDoneLocked()
}

func (p *pipe) closeDoneLocked() {
	if p.donec == nil {
		return
	}

	select {
	case <-p.donec:
	default:
		close(p.donec)
	}
}

func (p *pipe) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.breakErr != nil {
		return p.breakErr
	}
	return p.err
}

func (p *pipe) Done() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.donec == nil {
		p.donec = make(chan struct{})
		if p.err != nil || p.breakErr != nil {

			p.closeDoneLocked()
		}
	}
	return p.donec
}
