// Copyright 2014 The Go Authors. All rights reserved.

package http2

type flow struct {
	n int32

	conn *flow
}

func (f *flow) setConnFlow(cf *flow) { f.conn = cf }

func (f *flow) available() int32 {
	n := f.n
	if f.conn != nil && f.conn.n < n {
		n = f.conn.n
	}
	return n
}

func (f *flow) take(n int32) {
	if n > f.available() {
		panic("internal error: took too much")
	}
	f.n -= n
	if f.conn != nil {
		f.conn.n -= n
	}
}

func (f *flow) add(n int32) bool {
	remain := (1<<31 - 1) - f.n
	if n > remain {
		return false
	}
	f.n += n
	return true
}
