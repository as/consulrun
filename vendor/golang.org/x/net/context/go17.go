// Copyright 2016 The Go Authors. All rights reserved.

// +build go1.7

package context

import (
	"context" // standard library's context, as of Go 1.7
	"time"
)

var (
	todo       = context.TODO()
	background = context.Background()
)

var Canceled = context.Canceled

var DeadlineExceeded = context.DeadlineExceeded

//
func WithCancel(parent Context) (ctx Context, cancel CancelFunc) {
	ctx, f := context.WithCancel(parent)
	return ctx, CancelFunc(f)
}

//
func WithDeadline(parent Context, deadline time.Time) (Context, CancelFunc) {
	ctx, f := context.WithDeadline(parent, deadline)
	return ctx, CancelFunc(f)
}

//
//
func WithTimeout(parent Context, timeout time.Duration) (Context, CancelFunc) {
	return WithDeadline(parent, time.Now().Add(timeout))
}

//
func WithValue(parent Context, key interface{}, val interface{}) Context {
	return context.WithValue(parent, key, val)
}
