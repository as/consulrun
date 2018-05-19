// Copyright 2015 The Go Authors. All rights reserved.

package rate

import (
	"fmt"
	"math"
	"sync"
	"time"
)

type Limit float64

const Inf = Limit(math.MaxFloat64)

func Every(interval time.Duration) Limit {
	if interval <= 0 {
		return Inf
	}
	return 1 / Limit(interval.Seconds())
}

//
//
//
//
type Limiter struct {
	limit Limit
	burst int

	mu     sync.Mutex
	tokens float64

	last time.Time

	lastEvent time.Time
}

func (lim *Limiter) Limit() Limit {
	lim.mu.Lock()
	defer lim.mu.Unlock()
	return lim.limit
}

func (lim *Limiter) Burst() int {
	return lim.burst
}

func NewLimiter(r Limit, b int) *Limiter {
	return &Limiter{
		limit: r,
		burst: b,
	}
}

func (lim *Limiter) Allow() bool {
	return lim.AllowN(time.Now(), 1)
}

func (lim *Limiter) AllowN(now time.Time, n int) bool {
	return lim.reserveN(now, n, 0).ok
}

type Reservation struct {
	ok        bool
	lim       *Limiter
	tokens    int
	timeToAct time.Time

	limit Limit
}

func (r *Reservation) OK() bool {
	return r.ok
}

func (r *Reservation) Delay() time.Duration {
	return r.DelayFrom(time.Now())
}

const InfDuration = time.Duration(1<<63 - 1)

func (r *Reservation) DelayFrom(now time.Time) time.Duration {
	if !r.ok {
		return InfDuration
	}
	delay := r.timeToAct.Sub(now)
	if delay < 0 {
		return 0
	}
	return delay
}

func (r *Reservation) Cancel() {
	r.CancelAt(time.Now())
	return
}

func (r *Reservation) CancelAt(now time.Time) {
	if !r.ok {
		return
	}

	r.lim.mu.Lock()
	defer r.lim.mu.Unlock()

	if r.lim.limit == Inf || r.tokens == 0 || r.timeToAct.Before(now) {
		return
	}

	restoreTokens := float64(r.tokens) - r.limit.tokensFromDuration(r.lim.lastEvent.Sub(r.timeToAct))
	if restoreTokens <= 0 {
		return
	}

	now, _, tokens := r.lim.advance(now)

	tokens += restoreTokens
	if burst := float64(r.lim.burst); tokens > burst {
		tokens = burst
	}

	r.lim.last = now
	r.lim.tokens = tokens
	if r.timeToAct == r.lim.lastEvent {
		prevEvent := r.timeToAct.Add(r.limit.durationFromTokens(float64(-r.tokens)))
		if !prevEvent.Before(now) {
			r.lim.lastEvent = prevEvent
		}
	}

	return
}

func (lim *Limiter) Reserve() *Reservation {
	return lim.ReserveN(time.Now(), 1)
}

func (lim *Limiter) ReserveN(now time.Time, n int) *Reservation {
	r := lim.reserveN(now, n, InfDuration)
	return &r
}

type contextContext interface {
	Deadline() (deadline time.Time, ok bool)
	Done() <-chan struct{}
	Err() error
	Value(key interface{}) interface{}
}

func (lim *Limiter) wait(ctx contextContext) (err error) {
	return lim.WaitN(ctx, 1)
}

func (lim *Limiter) waitN(ctx contextContext, n int) (err error) {
	if n > lim.burst && lim.limit != Inf {
		return fmt.Errorf("rate: Wait(n=%d) exceeds limiter's burst %d", n, lim.burst)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	now := time.Now()
	waitLimit := InfDuration
	if deadline, ok := ctx.Deadline(); ok {
		waitLimit = deadline.Sub(now)
	}

	r := lim.reserveN(now, n, waitLimit)
	if !r.ok {
		return fmt.Errorf("rate: Wait(n=%d) would exceed context deadline", n)
	}

	t := time.NewTimer(r.DelayFrom(now))
	defer t.Stop()
	select {
	case <-t.C:

		return nil
	case <-ctx.Done():

		r.Cancel()
		return ctx.Err()
	}
}

func (lim *Limiter) SetLimit(newLimit Limit) {
	lim.SetLimitAt(time.Now(), newLimit)
}

func (lim *Limiter) SetLimitAt(now time.Time, newLimit Limit) {
	lim.mu.Lock()
	defer lim.mu.Unlock()

	now, _, tokens := lim.advance(now)

	lim.last = now
	lim.tokens = tokens
	lim.limit = newLimit
}

func (lim *Limiter) reserveN(now time.Time, n int, maxFutureReserve time.Duration) Reservation {
	lim.mu.Lock()

	if lim.limit == Inf {
		lim.mu.Unlock()
		return Reservation{
			ok:        true,
			lim:       lim,
			tokens:    n,
			timeToAct: now,
		}
	}

	now, last, tokens := lim.advance(now)

	tokens -= float64(n)

	var waitDuration time.Duration
	if tokens < 0 {
		waitDuration = lim.limit.durationFromTokens(-tokens)
	}

	ok := n <= lim.burst && waitDuration <= maxFutureReserve

	r := Reservation{
		ok:    ok,
		lim:   lim,
		limit: lim.limit,
	}
	if ok {
		r.tokens = n
		r.timeToAct = now.Add(waitDuration)
	}

	if ok {
		lim.last = now
		lim.tokens = tokens
		lim.lastEvent = r.timeToAct
	} else {
		lim.last = last
	}

	lim.mu.Unlock()
	return r
}

func (lim *Limiter) advance(now time.Time) (newNow time.Time, newLast time.Time, newTokens float64) {
	last := lim.last
	if now.Before(last) {
		last = now
	}

	maxElapsed := lim.limit.durationFromTokens(float64(lim.burst) - lim.tokens)
	elapsed := now.Sub(last)
	if elapsed > maxElapsed {
		elapsed = maxElapsed
	}

	delta := lim.limit.tokensFromDuration(elapsed)
	tokens := lim.tokens + delta
	if burst := float64(lim.burst); tokens > burst {
		tokens = burst
	}

	return now, last, tokens
}

func (limit Limit) durationFromTokens(tokens float64) time.Duration {
	seconds := tokens / float64(limit)
	return time.Nanosecond * time.Duration(1e9*seconds)
}

func (limit Limit) tokensFromDuration(d time.Duration) float64 {
	return d.Seconds() * float64(limit)
}
