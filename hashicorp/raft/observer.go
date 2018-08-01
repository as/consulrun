package raft

import (
	"sync/atomic"
)

type Observation struct {
	Raft *Raft

	Data interface{}
}

var nextObserverID uint64

type FilterFn func(o *Observation) bool

type Observer struct {
	numObserved uint64
	numDropped  uint64

	channel chan Observation

	blocking bool

	filter FilterFn

	id uint64
}

//
func NewObserver(channel chan Observation, blocking bool, filter FilterFn) *Observer {
	return &Observer{
		channel:  channel,
		blocking: blocking,
		filter:   filter,
		id:       atomic.AddUint64(&nextObserverID, 1),
	}
}

func (or *Observer) GetNumObserved() uint64 {
	return atomic.LoadUint64(&or.numObserved)
}

func (or *Observer) GetNumDropped() uint64 {
	return atomic.LoadUint64(&or.numDropped)
}

func (r *Raft) RegisterObserver(or *Observer) {
	r.observersLock.Lock()
	defer r.observersLock.Unlock()
	r.observers[or.id] = or
}

func (r *Raft) DeregisterObserver(or *Observer) {
	r.observersLock.Lock()
	defer r.observersLock.Unlock()
	delete(r.observers, or.id)
}

func (r *Raft) observe(o interface{}) {

	r.observersLock.RLock()
	defer r.observersLock.RUnlock()
	for _, or := range r.observers {

		ob := Observation{Raft: r, Data: o}
		if or.filter != nil && !or.filter(&ob) {
			continue
		}
		if or.channel == nil {
			continue
		}
		if or.blocking {
			or.channel <- ob
			atomic.AddUint64(&or.numObserved, 1)
		} else {
			select {
			case or.channel <- ob:
				atomic.AddUint64(&or.numObserved, 1)
			default:
				atomic.AddUint64(&or.numDropped, 1)
			}
		}
	}
}
