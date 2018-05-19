package raft

import (
	"fmt"
	"io"
	"time"

	"github.com/armon/go-metrics"
)

type FSM interface {
	Apply(*Log) interface{}

	Snapshot() (FSMSnapshot, error)

	Restore(io.ReadCloser) error
}

type FSMSnapshot interface {
	Persist(sink SnapshotSink) error

	Release()
}

func (r *Raft) runFSM() {
	var lastIndex, lastTerm uint64

	commit := func(req *commitTuple) {

		var resp interface{}
		if req.log.Type == LogCommand {
			start := time.Now()
			resp = r.fsm.Apply(req.log)
			metrics.MeasureSince([]string{"raft", "fsm", "apply"}, start)
		}

		lastIndex = req.log.Index
		lastTerm = req.log.Term

		if req.future != nil {
			req.future.response = resp
			req.future.respond(nil)
		}
	}

	restore := func(req *restoreFuture) {

		meta, source, err := r.snapshots.Open(req.ID)
		if err != nil {
			req.respond(fmt.Errorf("failed to open snapshot %v: %v", req.ID, err))
			return
		}

		start := time.Now()
		if err := r.fsm.Restore(source); err != nil {
			req.respond(fmt.Errorf("failed to restore snapshot %v: %v", req.ID, err))
			source.Close()
			return
		}
		source.Close()
		metrics.MeasureSince([]string{"raft", "fsm", "restore"}, start)

		lastIndex = meta.Index
		lastTerm = meta.Term
		req.respond(nil)
	}

	snapshot := func(req *reqSnapshotFuture) {

		if lastIndex == 0 {
			req.respond(ErrNothingNewToSnapshot)
			return
		}

		start := time.Now()
		snap, err := r.fsm.Snapshot()
		metrics.MeasureSince([]string{"raft", "fsm", "snapshot"}, start)

		req.index = lastIndex
		req.term = lastTerm
		req.snapshot = snap
		req.respond(err)
	}

	for {
		select {
		case ptr := <-r.fsmMutateCh:
			switch req := ptr.(type) {
			case *commitTuple:
				commit(req)

			case *restoreFuture:
				restore(req)

			default:
				panic(fmt.Errorf("bad type passed to fsmMutateCh: %#v", ptr))
			}

		case req := <-r.fsmSnapshotCh:
			snapshot(req)

		case <-r.shutdownCh:
			return
		}
	}
}
