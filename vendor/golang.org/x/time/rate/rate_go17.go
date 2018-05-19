// Copyright 2017 The Go Authors. All rights reserved.

// +build go1.7

package rate

import "context"

func (lim *Limiter) Wait(ctx context.Context) (err error) {
	return lim.waitN(ctx, 1)
}

func (lim *Limiter) WaitN(ctx context.Context, n int) (err error) {
	return lim.waitN(ctx, n)
}
