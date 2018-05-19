package raft

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/armon/go-metrics"
)

const (
	maxFailureScale = 12
	failureWait     = 10 * time.Millisecond
)

var (
	ErrLogNotFound = errors.New("log not found")

	ErrPipelineReplicationNotSupported = errors.New("pipeline replication not supported")
)

type followerReplication struct {
	peer Server

	commitment *commitment

	stopCh chan uint64

	triggerCh chan struct{}

	currentTerm uint64

	nextIndex uint64

	lastContact time.Time

	lastContactLock sync.RWMutex

	failures uint64

	notifyCh chan struct{}

	notify []*verifyFuture

	notifyLock sync.Mutex

	stepDown chan struct{}

	allowPipeline bool
}

func (s *followerReplication) notifyAll(leader bool) {

	s.notifyLock.Lock()
	n := s.notify
	s.notify = nil
	s.notifyLock.Unlock()

	for _, v := range n {
		v.vote(leader)
	}
}

func (s *followerReplication) LastContact() time.Time {
	s.lastContactLock.RLock()
	last := s.lastContact
	s.lastContactLock.RUnlock()
	return last
}

func (s *followerReplication) setLastContact() {
	s.lastContactLock.Lock()
	s.lastContact = time.Now()
	s.lastContactLock.Unlock()
}

func (r *Raft) replicate(s *followerReplication) {

	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	r.goFunc(func() { r.heartbeat(s, stopHeartbeat) })

RPC:
	shouldStop := false
	for !shouldStop {
		select {
		case maxIndex := <-s.stopCh:

			if maxIndex > 0 {
				r.replicateTo(s, maxIndex)
			}
			return
		case <-s.triggerCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(s, lastLogIdx)
		case <-randomTimeout(r.conf.CommitTimeout): // TODO: what is this?
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.replicateTo(s, lastLogIdx)
		}

		if !shouldStop && s.allowPipeline {
			goto PIPELINE
		}
	}
	return

PIPELINE:

	s.allowPipeline = false

	if err := r.pipelineReplicate(s); err != nil {
		if err != ErrPipelineReplicationNotSupported {
			r.logger.Printf("[ERR] raft: Failed to start pipeline replication to %s: %s", s.peer, err)
		}
	}
	goto RPC
}

func (r *Raft) replicateTo(s *followerReplication, lastIndex uint64) (shouldStop bool) {

	var req AppendEntriesRequest
	var resp AppendEntriesResponse
	var start time.Time
START:

	if s.failures > 0 {
		select {
		case <-time.After(backoff(failureWait, s.failures, maxFailureScale)):
		case <-r.shutdownCh:
		}
	}

	if err := r.setupAppendEntries(s, &req, s.nextIndex, lastIndex); err == ErrLogNotFound {
		goto SEND_SNAP
	} else if err != nil {
		return
	}

	start = time.Now()
	if err := r.trans.AppendEntries(s.peer.ID, s.peer.Address, &req, &resp); err != nil {
		r.logger.Printf("[ERR] raft: Failed to AppendEntries to %v: %v", s.peer, err)
		s.failures++
		return
	}
	appendStats(string(s.peer.ID), start, float32(len(req.Entries)))

	if resp.Term > req.Term {
		r.handleStaleTerm(s)
		return true
	}

	s.setLastContact()

	if resp.Success {

		updateLastAppended(s, &req)

		s.failures = 0
		s.allowPipeline = true
	} else {
		s.nextIndex = max(min(s.nextIndex-1, resp.LastLog+1), 1)
		if resp.NoRetryBackoff {
			s.failures = 0
		} else {
			s.failures++
		}
		r.logger.Printf("[WARN] raft: AppendEntries to %v rejected, sending older logs (next: %d)", s.peer, s.nextIndex)
	}

CHECK_MORE:

	select {
	case <-s.stopCh:
		return true
	default:
	}

	if s.nextIndex <= lastIndex {
		goto START
	}
	return

SEND_SNAP:
	if stop, err := r.sendLatestSnapshot(s); stop {
		return true
	} else if err != nil {
		r.logger.Printf("[ERR] raft: Failed to send snapshot to %v: %v", s.peer, err)
		return
	}

	goto CHECK_MORE
}

func (r *Raft) sendLatestSnapshot(s *followerReplication) (bool, error) {

	snapshots, err := r.snapshots.List()
	if err != nil {
		r.logger.Printf("[ERR] raft: Failed to list snapshots: %v", err)
		return false, err
	}

	if len(snapshots) == 0 {
		return false, fmt.Errorf("no snapshots found")
	}

	snapID := snapshots[0].ID
	meta, snapshot, err := r.snapshots.Open(snapID)
	if err != nil {
		r.logger.Printf("[ERR] raft: Failed to open snapshot %v: %v", snapID, err)
		return false, err
	}
	defer snapshot.Close()

	req := InstallSnapshotRequest{
		RPCHeader:          r.getRPCHeader(),
		SnapshotVersion:    meta.Version,
		Term:               s.currentTerm,
		Leader:             r.trans.EncodePeer(r.localID, r.localAddr),
		LastLogIndex:       meta.Index,
		LastLogTerm:        meta.Term,
		Peers:              meta.Peers,
		Size:               meta.Size,
		Configuration:      encodeConfiguration(meta.Configuration),
		ConfigurationIndex: meta.ConfigurationIndex,
	}

	start := time.Now()
	var resp InstallSnapshotResponse
	if err := r.trans.InstallSnapshot(s.peer.ID, s.peer.Address, &req, &resp, snapshot); err != nil {
		r.logger.Printf("[ERR] raft: Failed to install snapshot %v: %v", snapID, err)
		s.failures++
		return false, err
	}
	metrics.MeasureSince([]string{"raft", "replication", "installSnapshot", string(s.peer.ID)}, start)

	if resp.Term > req.Term {
		r.handleStaleTerm(s)
		return true, nil
	}

	s.setLastContact()

	if resp.Success {

		s.nextIndex = meta.Index + 1
		s.commitment.match(s.peer.ID, meta.Index)

		s.failures = 0

		s.notifyAll(true)
	} else {
		s.failures++
		r.logger.Printf("[WARN] raft: InstallSnapshot to %v rejected", s.peer)
	}
	return false, nil
}

func (r *Raft) heartbeat(s *followerReplication, stopCh chan struct{}) {
	var failures uint64
	req := AppendEntriesRequest{
		RPCHeader: r.getRPCHeader(),
		Term:      s.currentTerm,
		Leader:    r.trans.EncodePeer(r.localID, r.localAddr),
	}
	var resp AppendEntriesResponse
	for {

		select {
		case <-s.notifyCh:
		case <-randomTimeout(r.conf.HeartbeatTimeout / 10):
		case <-stopCh:
			return
		}

		start := time.Now()
		if err := r.trans.AppendEntries(s.peer.ID, s.peer.Address, &req, &resp); err != nil {
			r.logger.Printf("[ERR] raft: Failed to heartbeat to %v: %v", s.peer.Address, err)
			failures++
			select {
			case <-time.After(backoff(failureWait, failures, maxFailureScale)):
			case <-stopCh:
			}
		} else {
			s.setLastContact()
			failures = 0
			metrics.MeasureSince([]string{"raft", "replication", "heartbeat", string(s.peer.ID)}, start)
			s.notifyAll(resp.Success)
		}
	}
}

func (r *Raft) pipelineReplicate(s *followerReplication) error {

	pipeline, err := r.trans.AppendEntriesPipeline(s.peer.ID, s.peer.Address)
	if err != nil {
		return err
	}
	defer pipeline.Close()

	r.logger.Printf("[INFO] raft: pipelining replication to peer %v", s.peer)
	defer r.logger.Printf("[INFO] raft: aborting pipeline replication to peer %v", s.peer)

	stopCh := make(chan struct{})
	finishCh := make(chan struct{})

	r.goFunc(func() { r.pipelineDecode(s, pipeline, stopCh, finishCh) })

	nextIndex := s.nextIndex

	shouldStop := false
SEND:
	for !shouldStop {
		select {
		case <-finishCh:
			break SEND
		case maxIndex := <-s.stopCh:

			if maxIndex > 0 {
				r.pipelineSend(s, pipeline, &nextIndex, maxIndex)
			}
			break SEND
		case <-s.triggerCh:
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.pipelineSend(s, pipeline, &nextIndex, lastLogIdx)
		case <-randomTimeout(r.conf.CommitTimeout):
			lastLogIdx, _ := r.getLastLog()
			shouldStop = r.pipelineSend(s, pipeline, &nextIndex, lastLogIdx)
		}
	}

	close(stopCh)
	select {
	case <-finishCh:
	case <-r.shutdownCh:
	}
	return nil
}

func (r *Raft) pipelineSend(s *followerReplication, p AppendPipeline, nextIdx *uint64, lastIndex uint64) (shouldStop bool) {

	req := new(AppendEntriesRequest)
	if err := r.setupAppendEntries(s, req, *nextIdx, lastIndex); err != nil {
		return true
	}

	if _, err := p.AppendEntries(req, new(AppendEntriesResponse)); err != nil {
		r.logger.Printf("[ERR] raft: Failed to pipeline AppendEntries to %v: %v", s.peer, err)
		return true
	}

	if n := len(req.Entries); n > 0 {
		last := req.Entries[n-1]
		*nextIdx = last.Index + 1
	}
	return false
}

func (r *Raft) pipelineDecode(s *followerReplication, p AppendPipeline, stopCh, finishCh chan struct{}) {
	defer close(finishCh)
	respCh := p.Consumer()
	for {
		select {
		case ready := <-respCh:
			req, resp := ready.Request(), ready.Response()
			appendStats(string(s.peer.ID), ready.Start(), float32(len(req.Entries)))

			if resp.Term > req.Term {
				r.handleStaleTerm(s)
				return
			}

			s.setLastContact()

			if !resp.Success {
				return
			}

			updateLastAppended(s, req)
		case <-stopCh:
			return
		}
	}
}

func (r *Raft) setupAppendEntries(s *followerReplication, req *AppendEntriesRequest, nextIndex, lastIndex uint64) error {
	req.RPCHeader = r.getRPCHeader()
	req.Term = s.currentTerm
	req.Leader = r.trans.EncodePeer(r.localID, r.localAddr)
	req.LeaderCommitIndex = r.getCommitIndex()
	if err := r.setPreviousLog(req, nextIndex); err != nil {
		return err
	}
	if err := r.setNewLogs(req, nextIndex, lastIndex); err != nil {
		return err
	}
	return nil
}

func (r *Raft) setPreviousLog(req *AppendEntriesRequest, nextIndex uint64) error {

	lastSnapIdx, lastSnapTerm := r.getLastSnapshot()
	if nextIndex == 1 {
		req.PrevLogEntry = 0
		req.PrevLogTerm = 0

	} else if (nextIndex - 1) == lastSnapIdx {
		req.PrevLogEntry = lastSnapIdx
		req.PrevLogTerm = lastSnapTerm

	} else {
		var l Log
		if err := r.logs.GetLog(nextIndex-1, &l); err != nil {
			r.logger.Printf("[ERR] raft: Failed to get log at index %d: %v",
				nextIndex-1, err)
			return err
		}

		req.PrevLogEntry = l.Index
		req.PrevLogTerm = l.Term
	}
	return nil
}

func (r *Raft) setNewLogs(req *AppendEntriesRequest, nextIndex, lastIndex uint64) error {

	req.Entries = make([]*Log, 0, r.conf.MaxAppendEntries)
	maxIndex := min(nextIndex+uint64(r.conf.MaxAppendEntries)-1, lastIndex)
	for i := nextIndex; i <= maxIndex; i++ {
		oldLog := new(Log)
		if err := r.logs.GetLog(i, oldLog); err != nil {
			r.logger.Printf("[ERR] raft: Failed to get log at index %d: %v", i, err)
			return err
		}
		req.Entries = append(req.Entries, oldLog)
	}
	return nil
}

func appendStats(peer string, start time.Time, logs float32) {
	metrics.MeasureSince([]string{"raft", "replication", "appendEntries", "rpc", peer}, start)
	metrics.IncrCounter([]string{"raft", "replication", "appendEntries", "logs", peer}, logs)
}

func (r *Raft) handleStaleTerm(s *followerReplication) {
	r.logger.Printf("[ERR] raft: peer %v has newer term, stopping replication", s.peer)
	s.notifyAll(false) // No longer leader
	asyncNotifyCh(s.stepDown)
}

func updateLastAppended(s *followerReplication, req *AppendEntriesRequest) {

	if logs := req.Entries; len(logs) > 0 {
		last := logs[len(logs)-1]
		s.nextIndex = last.Index + 1
		s.commitment.match(s.peer.ID, last.Index)
	}

	s.notifyAll(true)
}
