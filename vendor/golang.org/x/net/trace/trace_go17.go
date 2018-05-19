// Copyright 2017 The Go Authors. All rights reserved.

// +build go1.7

package trace

import "context"

func NewContext(ctx context.Context, tr Trace) context.Context {
	return context.WithValue(ctx, contextKey, tr)
}

func FromContext(ctx context.Context) (tr Trace, ok bool) {
	tr, ok = ctx.Value(contextKey).(Trace)
	return
}
