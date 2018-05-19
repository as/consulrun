// Copyright 2014 The Go Authors. All rights reserved.

package http2

import "math"

// priorities. Control frames like SETTINGS and PING are written before DATA
// frames, but if no control frames are queued and multiple streams have queued
// HEADERS or DATA frames, Pop selects a ready stream arbitrarily.
func NewRandomWriteScheduler() WriteScheduler {
	return &randomWriteScheduler{sq: make(map[uint32]*writeQueue)}
}

type randomWriteScheduler struct {
	zero writeQueue

	sq map[uint32]*writeQueue

	queuePool writeQueuePool
}

func (ws *randomWriteScheduler) OpenStream(streamID uint32, options OpenStreamOptions) {

}

func (ws *randomWriteScheduler) CloseStream(streamID uint32) {
	q, ok := ws.sq[streamID]
	if !ok {
		return
	}
	delete(ws.sq, streamID)
	ws.queuePool.put(q)
}

func (ws *randomWriteScheduler) AdjustStream(streamID uint32, priority PriorityParam) {

}

func (ws *randomWriteScheduler) Push(wr FrameWriteRequest) {
	id := wr.StreamID()
	if id == 0 {
		ws.zero.push(wr)
		return
	}
	q, ok := ws.sq[id]
	if !ok {
		q = ws.queuePool.get()
		ws.sq[id] = q
	}
	q.push(wr)
}

func (ws *randomWriteScheduler) Pop() (FrameWriteRequest, bool) {

	if !ws.zero.empty() {
		return ws.zero.shift(), true
	}

	for _, q := range ws.sq {
		if wr, ok := q.consume(math.MaxInt32); ok {
			return wr, true
		}
	}
	return FrameWriteRequest{}, false
}
