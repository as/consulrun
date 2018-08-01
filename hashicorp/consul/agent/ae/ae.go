// Package ae provides tools to synchronize state between local and remote consul servers.
package ae

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/as/consulrun/hashicorp/consul/lib"
)

//
//
const scaleThreshold = 128

//
func scaleFactor(nodes int) int {
	if nodes <= scaleThreshold {
		return 1.0
	}
	return int(math.Ceil(math.Log2(float64(nodes))-math.Log2(float64(scaleThreshold))) + 1.0)
}

type SyncState interface {
	SyncChanges() error
	SyncFull() error
}

//
//
type StateSyncer struct {
	State SyncState

	Interval time.Duration

	ShutdownCh chan struct{}

	Logger *log.Logger

	ClusterSize func() int

	SyncFull *Trigger

	SyncChanges *Trigger

	pauseLock sync.Mutex
	paused    int

	serverUpInterval time.Duration

	retryFailInterval time.Duration

	stagger func(time.Duration) time.Duration

	retrySyncFullEvent func() event

	syncChangesEvent func() event
}

const (
	serverUpIntv = 3 * time.Second

	retryFailIntv = 15 * time.Second
)

func NewStateSyncer(state SyncState, intv time.Duration, shutdownCh chan struct{}, logger *log.Logger) *StateSyncer {
	s := &StateSyncer{
		State:             state,
		Interval:          intv,
		ShutdownCh:        shutdownCh,
		Logger:            logger,
		SyncFull:          NewTrigger(),
		SyncChanges:       NewTrigger(),
		serverUpInterval:  serverUpIntv,
		retryFailInterval: retryFailIntv,
	}

	s.retrySyncFullEvent = s.retrySyncFullEventFn
	s.syncChangesEvent = s.syncChangesEventFn
	s.stagger = s.staggerFn

	return s
}

type fsmState string

const (
	doneState          fsmState = "done"
	fullSyncState      fsmState = "fullSync"
	partialSyncState   fsmState = "partialSync"
	retryFullSyncState fsmState = "retryFullSync"
)

func (s *StateSyncer) Run() {
	if s.ClusterSize == nil {
		panic("ClusterSize not set")
	}
	s.runFSM(fullSyncState, s.nextFSMState)
}

func (s *StateSyncer) runFSM(fs fsmState, next func(fsmState) fsmState) {
	for {
		if fs = next(fs); fs == doneState {
			return
		}
	}
}

func (s *StateSyncer) nextFSMState(fs fsmState) fsmState {
	switch fs {
	case fullSyncState:
		if s.Paused() {
			return retryFullSyncState
		}

		err := s.State.SyncFull()
		if err != nil {
			s.Logger.Printf("[ERR] agent: failed to sync remote state: %v", err)
			return retryFullSyncState
		}

		return partialSyncState

	case retryFullSyncState:
		e := s.retrySyncFullEvent()
		switch e {
		case syncFullNotifEvent, syncFullTimerEvent:
			return fullSyncState
		case shutdownEvent:
			return doneState
		default:
			panic(fmt.Sprintf("invalid event: %s", e))
		}

	case partialSyncState:
		e := s.syncChangesEvent()
		switch e {
		case syncFullNotifEvent, syncFullTimerEvent:
			return fullSyncState

		case syncChangesNotifEvent:
			if s.Paused() {
				return partialSyncState
			}

			err := s.State.SyncChanges()
			if err != nil {
				s.Logger.Printf("[ERR] agent: failed to sync changes: %v", err)
			}
			return partialSyncState

		case shutdownEvent:
			return doneState

		default:
			panic(fmt.Sprintf("invalid event: %s", e))
		}

	default:
		panic(fmt.Sprintf("invalid state: %s", fs))
	}
}

type event string

const (
	shutdownEvent         event = "shutdown"
	syncFullNotifEvent    event = "syncFullNotif"
	syncFullTimerEvent    event = "syncFullTimer"
	syncChangesNotifEvent event = "syncChangesNotif"
)

func (s *StateSyncer) retrySyncFullEventFn() event {
	select {

	case <-s.SyncFull.Notif():
		select {
		case <-time.After(s.stagger(s.serverUpInterval)):
			return syncFullNotifEvent
		case <-s.ShutdownCh:
			return shutdownEvent
		}

	case <-time.After(s.retryFailInterval + s.stagger(s.retryFailInterval)):
		return syncFullTimerEvent

	case <-s.ShutdownCh:
		return shutdownEvent
	}
}

func (s *StateSyncer) syncChangesEventFn() event {
	select {

	case <-s.SyncFull.Notif():
		select {
		case <-time.After(s.stagger(s.serverUpInterval)):
			return syncFullNotifEvent
		case <-s.ShutdownCh:
			return shutdownEvent
		}

	case <-time.After(s.Interval + s.stagger(s.Interval)):
		return syncFullTimerEvent

	case <-s.SyncChanges.Notif():
		return syncChangesNotifEvent

	case <-s.ShutdownCh:
		return shutdownEvent
	}
}

var libRandomStagger = lib.RandomStagger

func (s *StateSyncer) staggerFn(d time.Duration) time.Duration {
	f := scaleFactor(s.ClusterSize())
	return libRandomStagger(time.Duration(f) * d)
}

func (s *StateSyncer) Pause() {
	s.pauseLock.Lock()
	s.paused++
	s.pauseLock.Unlock()
}

func (s *StateSyncer) Paused() bool {
	s.pauseLock.Lock()
	defer s.pauseLock.Unlock()
	return s.paused != 0
}

func (s *StateSyncer) Resume() {
	s.pauseLock.Lock()
	s.paused--
	if s.paused < 0 {
		panic("unbalanced pause/resume")
	}
	trigger := s.paused == 0
	s.pauseLock.Unlock()
	if trigger {
		s.SyncChanges.Trigger()
	}
}
