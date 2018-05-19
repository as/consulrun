package serf

import (
	"time"
)

type coalescer interface {
	Handle(Event) bool

	Coalesce(Event)

	Flush(outChan chan<- Event)
}

func coalescedEventCh(outCh chan<- Event, shutdownCh <-chan struct{},
	cPeriod time.Duration, qPeriod time.Duration, c coalescer) chan<- Event {
	inCh := make(chan Event, 1024)
	go coalesceLoop(inCh, outCh, shutdownCh, cPeriod, qPeriod, c)
	return inCh
}

func coalesceLoop(inCh <-chan Event, outCh chan<- Event, shutdownCh <-chan struct{},
	coalescePeriod time.Duration, quiescentPeriod time.Duration, c coalescer) {
	var quiescent <-chan time.Time
	var quantum <-chan time.Time
	shutdown := false

INGEST:

	quantum = nil
	quiescent = nil

	for {
		select {
		case e := <-inCh:

			if !c.Handle(e) {
				outCh <- e
				continue
			}

			if quantum == nil {
				quantum = time.After(coalescePeriod)
			}
			quiescent = time.After(quiescentPeriod)

			c.Coalesce(e)

		case <-quantum:
			goto FLUSH
		case <-quiescent:
			goto FLUSH
		case <-shutdownCh:
			shutdown = true
			goto FLUSH
		}
	}

FLUSH:

	c.Flush(outCh)

	if !shutdown {
		goto INGEST
	}
}
