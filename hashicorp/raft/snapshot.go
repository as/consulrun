package raft

import (
	"fmt"
	"io"
	"time"

	"github.com/armon/go-metrics"
)

type SnapshotMeta struct {
	Version SnapshotVersion

	ID string

	Index uint64
	Term  uint64

	Peers []byte

	Configuration      Configuration
	ConfigurationIndex uint64

	Size int64
}

type SnapshotStore interface {
	Create(version SnapshotVersion, index, term uint64, configuration Configuration,
		configurationIndex uint64, trans Transport) (SnapshotSink, error)

	List() ([]*SnapshotMeta, error)

	Open(id string) (*SnapshotMeta, io.ReadCloser, error)
}

type SnapshotSink interface {
	io.WriteCloser
	ID() string
	Cancel() error
}

func (r *Raft) runSnapshots() {
	for {
		select {
		case <-randomTimeout(r.conf.SnapshotInterval):

			if !r.shouldSnapshot() {
				continue
			}

			if _, err := r.takeSnapshot(); err != nil {
				r.logger.Printf("[ERR] raft: Failed to take snapshot: %v", err)
			}

		case future := <-r.userSnapshotCh:

			id, err := r.takeSnapshot()
			if err != nil {
				r.logger.Printf("[ERR] raft: Failed to take snapshot: %v", err)
			} else {
				future.opener = func() (*SnapshotMeta, io.ReadCloser, error) {
					return r.snapshots.Open(id)
				}
			}
			future.respond(err)

		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) shouldSnapshot() bool {

	lastSnap, _ := r.getLastSnapshot()

	lastIdx, err := r.logs.LastIndex()
	if err != nil {
		r.logger.Printf("[ERR] raft: Failed to get last log index: %v", err)
		return false
	}

	delta := lastIdx - lastSnap
	return delta >= r.conf.SnapshotThreshold
}

func (r *Raft) takeSnapshot() (string, error) {
	defer metrics.MeasureSince([]string{"raft", "snapshot", "takeSnapshot"}, time.Now())

	snapReq := &reqSnapshotFuture{}
	snapReq.init()

	select {
	case r.fsmSnapshotCh <- snapReq:
	case <-r.shutdownCh:
		return "", ErrRaftShutdown
	}

	if err := snapReq.Error(); err != nil {
		if err != ErrNothingNewToSnapshot {
			err = fmt.Errorf("failed to start snapshot: %v", err)
		}
		return "", err
	}
	defer snapReq.snapshot.Release()

	configReq := &configurationsFuture{}
	configReq.init()
	select {
	case r.configurationsCh <- configReq:
	case <-r.shutdownCh:
		return "", ErrRaftShutdown
	}
	if err := configReq.Error(); err != nil {
		return "", err
	}
	committed := configReq.configurations.committed
	committedIndex := configReq.configurations.committedIndex

	if snapReq.index < committedIndex {
		return "", fmt.Errorf("cannot take snapshot now, wait until the configuration entry at %v has been applied (have applied %v)",
			committedIndex, snapReq.index)
	}

	r.logger.Printf("[INFO] raft: Starting snapshot up to %d", snapReq.index)
	start := time.Now()
	version := getSnapshotVersion(r.protocolVersion)
	sink, err := r.snapshots.Create(version, snapReq.index, snapReq.term, committed, committedIndex, r.trans)
	if err != nil {
		return "", fmt.Errorf("failed to create snapshot: %v", err)
	}
	metrics.MeasureSince([]string{"raft", "snapshot", "create"}, start)

	start = time.Now()
	if err := snapReq.snapshot.Persist(sink); err != nil {
		sink.Cancel()
		return "", fmt.Errorf("failed to persist snapshot: %v", err)
	}
	metrics.MeasureSince([]string{"raft", "snapshot", "persist"}, start)

	if err := sink.Close(); err != nil {
		return "", fmt.Errorf("failed to close snapshot: %v", err)
	}

	r.setLastSnapshot(snapReq.index, snapReq.term)

	if err := r.compactLogs(snapReq.index); err != nil {
		return "", err
	}

	r.logger.Printf("[INFO] raft: Snapshot to %d complete", snapReq.index)
	return sink.ID(), nil
}

func (r *Raft) compactLogs(snapIdx uint64) error {
	defer metrics.MeasureSince([]string{"raft", "compactLogs"}, time.Now())

	minLog, err := r.logs.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get first log index: %v", err)
	}

	lastLogIdx, _ := r.getLastLog()
	if lastLogIdx <= r.conf.TrailingLogs {
		return nil
	}

	maxLog := min(snapIdx, lastLogIdx-r.conf.TrailingLogs)

	r.logger.Printf("[INFO] raft: Compacting logs from %d to %d", minLog, maxLog)

	if err := r.logs.DeleteRange(minLog, maxLog); err != nil {
		return fmt.Errorf("log compaction failed: %v", err)
	}
	return nil
}
