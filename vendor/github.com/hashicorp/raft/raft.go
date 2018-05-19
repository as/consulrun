package raft

import (
	"bytes"
	"container/list"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/armon/go-metrics"
)

const (
	minCheckInterval = 10 * time.Millisecond
)

var (
	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
)

func (r *Raft) getRPCHeader() RPCHeader {
	return RPCHeader{
		ProtocolVersion: r.conf.ProtocolVersion,
	}
}

func (r *Raft) checkRPCHeader(rpc RPC) error {

	wh, ok := rpc.Command.(WithRPCHeader)
	if !ok {
		return fmt.Errorf("RPC does not have a header")
	}
	header := wh.GetRPCHeader()

	if header.ProtocolVersion < ProtocolVersionMin ||
		header.ProtocolVersion > ProtocolVersionMax {
		return ErrUnsupportedProtocol
	}

	if header.ProtocolVersion < r.conf.ProtocolVersion-1 {
		return ErrUnsupportedProtocol
	}

	return nil
}

func getSnapshotVersion(protocolVersion ProtocolVersion) SnapshotVersion {

	return 1
}

type commitTuple struct {
	log    *Log
	future *logFuture
}

type leaderState struct {
	commitCh   chan struct{}
	commitment *commitment
	inflight   *list.List // list of logFuture in log index order
	replState  map[ServerID]*followerReplication
	notify     map[*verifyFuture]struct{}
	stepDown   chan struct{}
}

func (r *Raft) setLeader(leader ServerAddress) {
	r.leaderLock.Lock()
	r.leader = leader
	r.leaderLock.Unlock()
}

func (r *Raft) requestConfigChange(req configurationChangeRequest, timeout time.Duration) IndexFuture {
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}
	future := &configurationChangeFuture{
		req: req,
	}
	future.init()
	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
	case r.configurationChangeCh <- future:
		return future
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	}
}

func (r *Raft) run() {
	for {

		select {
		case <-r.shutdownCh:

			r.setLeader("")
			return
		default:
		}

		switch r.getState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

func (r *Raft) runFollower() {
	didWarn := false
	r.logger.Printf("[INFO] raft: %v entering Follower state (Leader: %q)", r, r.Leader())
	metrics.IncrCounter([]string{"raft", "state", "follower"}, 1)
	heartbeatTimer := randomTimeout(r.conf.HeartbeatTimeout)
	for {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		case c := <-r.configurationChangeCh:

			c.respond(ErrNotLeader)

		case a := <-r.applyCh:

			a.respond(ErrNotLeader)

		case v := <-r.verifyCh:

			v.respond(ErrNotLeader)

		case r := <-r.userRestoreCh:

			r.respond(ErrNotLeader)

		case c := <-r.configurationsCh:
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case b := <-r.bootstrapCh:
			b.respond(r.liveBootstrap(b.configuration))

		case <-heartbeatTimer:

			heartbeatTimer = randomTimeout(r.conf.HeartbeatTimeout)

			lastContact := r.LastContact()
			if time.Now().Sub(lastContact) < r.conf.HeartbeatTimeout {
				continue
			}

			lastLeader := r.Leader()
			r.setLeader("")

			if r.configurations.latestIndex == 0 {
				if !didWarn {
					r.logger.Printf("[WARN] raft: no known peers, aborting election")
					didWarn = true
				}
			} else if r.configurations.latestIndex == r.configurations.committedIndex &&
				!hasVote(r.configurations.latest, r.localID) {
				if !didWarn {
					r.logger.Printf("[WARN] raft: not part of stable configuration, aborting election")
					didWarn = true
				}
			} else {
				r.logger.Printf(`[WARN] raft: Heartbeat timeout from %q reached, starting election`, lastLeader)
				metrics.IncrCounter([]string{"raft", "transition", "heartbeat_timeout"}, 1)
				r.setState(Candidate)
				return
			}

		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) liveBootstrap(configuration Configuration) error {

	err := BootstrapCluster(&r.conf, r.logs, r.stable, r.snapshots,
		r.trans, configuration)
	if err != nil {
		return err
	}

	var entry Log
	if err := r.logs.GetLog(1, &entry); err != nil {
		panic(err)
	}
	r.setCurrentTerm(1)
	r.setLastLog(entry.Index, entry.Term)
	r.processConfigurationLogEntry(&entry)
	return nil
}

func (r *Raft) runCandidate() {
	r.logger.Printf("[INFO] raft: %v entering Candidate state in term %v",
		r, r.getCurrentTerm()+1)
	metrics.IncrCounter([]string{"raft", "state", "candidate"}, 1)

	voteCh := r.electSelf()
	electionTimer := randomTimeout(r.conf.ElectionTimeout)

	grantedVotes := 0
	votesNeeded := r.quorumSize()
	r.logger.Printf("[DEBUG] raft: Votes needed: %d", votesNeeded)

	for r.getState() == Candidate {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		case vote := <-voteCh:

			if vote.Term > r.getCurrentTerm() {
				r.logger.Printf("[DEBUG] raft: Newer term discovered, fallback to follower")
				r.setState(Follower)
				r.setCurrentTerm(vote.Term)
				return
			}

			if vote.Granted {
				grantedVotes++
				r.logger.Printf("[DEBUG] raft: Vote granted from %s in term %v. Tally: %d",
					vote.voterID, vote.Term, grantedVotes)
			}

			if grantedVotes >= votesNeeded {
				r.logger.Printf("[INFO] raft: Election won. Tally: %d", grantedVotes)
				r.setState(Leader)
				r.setLeader(r.localAddr)
				return
			}

		case c := <-r.configurationChangeCh:

			c.respond(ErrNotLeader)

		case a := <-r.applyCh:

			a.respond(ErrNotLeader)

		case v := <-r.verifyCh:

			v.respond(ErrNotLeader)

		case r := <-r.userRestoreCh:

			r.respond(ErrNotLeader)

		case c := <-r.configurationsCh:
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case b := <-r.bootstrapCh:
			b.respond(ErrCantBootstrap)

		case <-electionTimer:

			r.logger.Printf("[WARN] raft: Election timeout reached, restarting election")
			return

		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) runLeader() {
	r.logger.Printf("[INFO] raft: %v entering Leader state", r)
	metrics.IncrCounter([]string{"raft", "state", "leader"}, 1)

	asyncNotifyBool(r.leaderCh, true)

	if notify := r.conf.NotifyCh; notify != nil {
		select {
		case notify <- true:
		case <-r.shutdownCh:
		}
	}

	r.leaderState.commitCh = make(chan struct{}, 1)
	r.leaderState.commitment = newCommitment(r.leaderState.commitCh,
		r.configurations.latest,
		r.getLastIndex()+1 /* first index that may be committed in this term */)
	r.leaderState.inflight = list.New()
	r.leaderState.replState = make(map[ServerID]*followerReplication)
	r.leaderState.notify = make(map[*verifyFuture]struct{})
	r.leaderState.stepDown = make(chan struct{}, 1)

	defer func() {

		r.setLastContact()

		for _, p := range r.leaderState.replState {
			close(p.stopCh)
		}

		for e := r.leaderState.inflight.Front(); e != nil; e = e.Next() {
			e.Value.(*logFuture).respond(ErrLeadershipLost)
		}

		for future := range r.leaderState.notify {
			future.respond(ErrLeadershipLost)
		}

		r.leaderState.commitCh = nil
		r.leaderState.commitment = nil
		r.leaderState.inflight = nil
		r.leaderState.replState = nil
		r.leaderState.notify = nil
		r.leaderState.stepDown = nil

		r.leaderLock.Lock()
		if r.leader == r.localAddr {
			r.leader = ""
		}
		r.leaderLock.Unlock()

		asyncNotifyBool(r.leaderCh, false)

		if notify := r.conf.NotifyCh; notify != nil {
			select {
			case notify <- false:
			case <-r.shutdownCh:

				select {
				case notify <- false:
				default:
				}
			}
		}
	}()

	r.startStopReplication()

	noop := &logFuture{
		log: Log{
			Type: LogNoop,
		},
	}
	r.dispatchLogs([]*logFuture{noop})

	r.leaderLoop()
}

func (r *Raft) startStopReplication() {
	inConfig := make(map[ServerID]bool, len(r.configurations.latest.Servers))
	lastIdx := r.getLastIndex()

	for _, server := range r.configurations.latest.Servers {
		if server.ID == r.localID {
			continue
		}
		inConfig[server.ID] = true
		if _, ok := r.leaderState.replState[server.ID]; !ok {
			r.logger.Printf("[INFO] raft: Added peer %v, starting replication", server.ID)
			s := &followerReplication{
				peer:        server,
				commitment:  r.leaderState.commitment,
				stopCh:      make(chan uint64, 1),
				triggerCh:   make(chan struct{}, 1),
				currentTerm: r.getCurrentTerm(),
				nextIndex:   lastIdx + 1,
				lastContact: time.Now(),
				notifyCh:    make(chan struct{}, 1),
				stepDown:    r.leaderState.stepDown,
			}
			r.leaderState.replState[server.ID] = s
			r.goFunc(func() { r.replicate(s) })
			asyncNotifyCh(s.triggerCh)
		}
	}

	for serverID, repl := range r.leaderState.replState {
		if inConfig[serverID] {
			continue
		}

		r.logger.Printf("[INFO] raft: Removed peer %v, stopping replication after %v", serverID, lastIdx)
		repl.stopCh <- lastIdx
		close(repl.stopCh)
		delete(r.leaderState.replState, serverID)
	}
}

//
func (r *Raft) configurationChangeChIfStable() chan *configurationChangeFuture {

	if r.configurations.latestIndex == r.configurations.committedIndex &&
		r.getCommitIndex() >= r.leaderState.commitment.startIndex {
		return r.configurationChangeCh
	}
	return nil
}

func (r *Raft) leaderLoop() {

	stepDown := false

	lease := time.After(r.conf.LeaderLeaseTimeout)
	for r.getState() == Leader {
		select {
		case rpc := <-r.rpcCh:
			r.processRPC(rpc)

		case <-r.leaderState.stepDown:
			r.setState(Follower)

		case <-r.leaderState.commitCh:

			oldCommitIndex := r.getCommitIndex()
			commitIndex := r.leaderState.commitment.getCommitIndex()
			r.setCommitIndex(commitIndex)

			if r.configurations.latestIndex > oldCommitIndex &&
				r.configurations.latestIndex <= commitIndex {
				r.configurations.committed = r.configurations.latest
				r.configurations.committedIndex = r.configurations.latestIndex
				if !hasVote(r.configurations.committed, r.localID) {
					stepDown = true
				}
			}

			for {
				e := r.leaderState.inflight.Front()
				if e == nil {
					break
				}
				commitLog := e.Value.(*logFuture)
				idx := commitLog.log.Index
				if idx > commitIndex {
					break
				}

				metrics.MeasureSince([]string{"raft", "commitTime"}, commitLog.dispatch)
				r.processLogs(idx, commitLog)
				r.leaderState.inflight.Remove(e)
			}

			if stepDown {
				if r.conf.ShutdownOnRemove {
					r.logger.Printf("[INFO] raft: Removed ourself, shutting down")
					r.Shutdown()
				} else {
					r.logger.Printf("[INFO] raft: Removed ourself, transitioning to follower")
					r.setState(Follower)
				}
			}

		case v := <-r.verifyCh:
			if v.quorumSize == 0 {

				r.verifyLeader(v)

			} else if v.votes < v.quorumSize {

				r.logger.Printf("[WARN] raft: New leader elected, stepping down")
				r.setState(Follower)
				delete(r.leaderState.notify, v)
				v.respond(ErrNotLeader)

			} else {

				delete(r.leaderState.notify, v)
				v.respond(nil)
			}

		case future := <-r.userRestoreCh:
			err := r.restoreUserSnapshot(future.meta, future.reader)
			future.respond(err)

		case c := <-r.configurationsCh:
			c.configurations = r.configurations.Clone()
			c.respond(nil)

		case future := <-r.configurationChangeChIfStable():
			r.appendConfigurationEntry(future)

		case b := <-r.bootstrapCh:
			b.respond(ErrCantBootstrap)

		case newLog := <-r.applyCh:

			ready := []*logFuture{newLog}
			for i := 0; i < r.conf.MaxAppendEntries; i++ {
				select {
				case newLog := <-r.applyCh:
					ready = append(ready, newLog)
				default:
					break
				}
			}

			if stepDown {

				for i := range ready {
					ready[i].respond(ErrNotLeader)
				}
			} else {
				r.dispatchLogs(ready)
			}

		case <-lease:

			maxDiff := r.checkLeaderLease()

			checkInterval := r.conf.LeaderLeaseTimeout - maxDiff
			if checkInterval < minCheckInterval {
				checkInterval = minCheckInterval
			}

			lease = time.After(checkInterval)

		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) verifyLeader(v *verifyFuture) {

	v.votes = 1

	v.quorumSize = r.quorumSize()
	if v.quorumSize == 1 {
		v.respond(nil)
		return
	}

	v.notifyCh = r.verifyCh
	r.leaderState.notify[v] = struct{}{}

	for _, repl := range r.leaderState.replState {
		repl.notifyLock.Lock()
		repl.notify = append(repl.notify, v)
		repl.notifyLock.Unlock()
		asyncNotifyCh(repl.notifyCh)
	}
}

func (r *Raft) checkLeaderLease() time.Duration {

	contacted := 1

	var maxDiff time.Duration
	now := time.Now()
	for peer, f := range r.leaderState.replState {
		diff := now.Sub(f.LastContact())
		if diff <= r.conf.LeaderLeaseTimeout {
			contacted++
			if diff > maxDiff {
				maxDiff = diff
			}
		} else {

			if diff <= 3*r.conf.LeaderLeaseTimeout {
				r.logger.Printf("[WARN] raft: Failed to contact %v in %v", peer, diff)
			} else {
				r.logger.Printf("[DEBUG] raft: Failed to contact %v in %v", peer, diff)
			}
		}
		metrics.AddSample([]string{"raft", "leader", "lastContact"}, float32(diff/time.Millisecond))
	}

	quorum := r.quorumSize()
	if contacted < quorum {
		r.logger.Printf("[WARN] raft: Failed to contact quorum of nodes, stepping down")
		r.setState(Follower)
		metrics.IncrCounter([]string{"raft", "transition", "leader_lease_timeout"}, 1)
	}
	return maxDiff
}

func (r *Raft) quorumSize() int {
	voters := 0
	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			voters++
		}
	}
	return voters/2 + 1
}

func (r *Raft) restoreUserSnapshot(meta *SnapshotMeta, reader io.Reader) error {
	defer metrics.MeasureSince([]string{"raft", "restoreUserSnapshot"}, time.Now())

	version := meta.Version
	if version < SnapshotVersionMin || version > SnapshotVersionMax {
		return fmt.Errorf("unsupported snapshot version %d", version)
	}

	committedIndex := r.configurations.committedIndex
	latestIndex := r.configurations.latestIndex
	if committedIndex != latestIndex {
		return fmt.Errorf("cannot restore snapshot now, wait until the configuration entry at %v has been applied (have applied %v)",
			latestIndex, committedIndex)
	}

	for {
		e := r.leaderState.inflight.Front()
		if e == nil {
			break
		}
		e.Value.(*logFuture).respond(ErrAbortedByRestore)
		r.leaderState.inflight.Remove(e)
	}

	term := r.getCurrentTerm()
	lastIndex := r.getLastIndex()
	if meta.Index > lastIndex {
		lastIndex = meta.Index
	}
	lastIndex++

	sink, err := r.snapshots.Create(version, lastIndex, term,
		r.configurations.latest, r.configurations.latestIndex, r.trans)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}
	n, err := io.Copy(sink, reader)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to write snapshot: %v", err)
	}
	if n != meta.Size {
		sink.Cancel()
		return fmt.Errorf("failed to write snapshot, size didn't match (%d != %d)", n, meta.Size)
	}
	if err := sink.Close(); err != nil {
		return fmt.Errorf("failed to close snapshot: %v", err)
	}
	r.logger.Printf("[INFO] raft: Copied %d bytes to local snapshot", n)

	fsm := &restoreFuture{ID: sink.ID()}
	fsm.init()
	select {
	case r.fsmMutateCh <- fsm:
	case <-r.shutdownCh:
		return ErrRaftShutdown
	}
	if err := fsm.Error(); err != nil {
		panic(fmt.Errorf("failed to restore snapshot: %v", err))
	}

	r.setLastLog(lastIndex, term)
	r.setLastApplied(lastIndex)
	r.setLastSnapshot(lastIndex, term)

	r.logger.Printf("[INFO] raft: Restored user snapshot (index %d)", lastIndex)
	return nil
}

func (r *Raft) appendConfigurationEntry(future *configurationChangeFuture) {
	configuration, err := nextConfiguration(r.configurations.latest, r.configurations.latestIndex, future.req)
	if err != nil {
		future.respond(err)
		return
	}

	r.logger.Printf("[INFO] raft: Updating configuration with %s (%v, %v) to %+v",
		future.req.command, future.req.serverID, future.req.serverAddress, configuration.Servers)

	if r.protocolVersion < 2 {
		future.log = Log{
			Type: LogRemovePeerDeprecated,
			Data: encodePeers(configuration, r.trans),
		}
	} else {
		future.log = Log{
			Type: LogConfiguration,
			Data: encodeConfiguration(configuration),
		}
	}

	r.dispatchLogs([]*logFuture{&future.logFuture})
	index := future.Index()
	r.configurations.latest = configuration
	r.configurations.latestIndex = index
	r.leaderState.commitment.setConfiguration(configuration)
	r.startStopReplication()
}

func (r *Raft) dispatchLogs(applyLogs []*logFuture) {
	now := time.Now()
	defer metrics.MeasureSince([]string{"raft", "leader", "dispatchLog"}, now)

	term := r.getCurrentTerm()
	lastIndex := r.getLastIndex()
	logs := make([]*Log, len(applyLogs))

	for idx, applyLog := range applyLogs {
		applyLog.dispatch = now
		lastIndex++
		applyLog.log.Index = lastIndex
		applyLog.log.Term = term
		logs[idx] = &applyLog.log
		r.leaderState.inflight.PushBack(applyLog)
	}

	if err := r.logs.StoreLogs(logs); err != nil {
		r.logger.Printf("[ERR] raft: Failed to commit logs: %v", err)
		for _, applyLog := range applyLogs {
			applyLog.respond(err)
		}
		r.setState(Follower)
		return
	}
	r.leaderState.commitment.match(r.localID, lastIndex)

	r.setLastLog(lastIndex, term)

	for _, f := range r.leaderState.replState {
		asyncNotifyCh(f.triggerCh)
	}
}

func (r *Raft) processLogs(index uint64, future *logFuture) {

	lastApplied := r.getLastApplied()
	if index <= lastApplied {
		r.logger.Printf("[WARN] raft: Skipping application of old log: %d", index)
		return
	}

	for idx := r.getLastApplied() + 1; idx <= index; idx++ {

		if future != nil && future.log.Index == idx {
			r.processLog(&future.log, future)

		} else {
			l := new(Log)
			if err := r.logs.GetLog(idx, l); err != nil {
				r.logger.Printf("[ERR] raft: Failed to get log at %d: %v", idx, err)
				panic(err)
			}
			r.processLog(l, nil)
		}

		r.setLastApplied(idx)
	}
}

func (r *Raft) processLog(l *Log, future *logFuture) {
	switch l.Type {
	case LogBarrier:

		fallthrough

	case LogCommand:

		select {
		case r.fsmMutateCh <- &commitTuple{l, future}:
		case <-r.shutdownCh:
			if future != nil {
				future.respond(ErrRaftShutdown)
			}
		}

		return

	case LogConfiguration:
	case LogAddPeerDeprecated:
	case LogRemovePeerDeprecated:
	case LogNoop:

	default:
		panic(fmt.Errorf("unrecognized log type: %#v", l))
	}

	if future != nil {
		future.respond(nil)
	}
}

func (r *Raft) processRPC(rpc RPC) {
	if err := r.checkRPCHeader(rpc); err != nil {
		rpc.Respond(nil, err)
		return
	}

	switch cmd := rpc.Command.(type) {
	case *AppendEntriesRequest:
		r.appendEntries(rpc, cmd)
	case *RequestVoteRequest:
		r.requestVote(rpc, cmd)
	case *InstallSnapshotRequest:
		r.installSnapshot(rpc, cmd)
	default:
		r.logger.Printf("[ERR] raft: Got unexpected command: %#v", rpc.Command)
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

func (r *Raft) processHeartbeat(rpc RPC) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "processHeartbeat"}, time.Now())

	select {
	case <-r.shutdownCh:
		return
	default:
	}

	switch cmd := rpc.Command.(type) {
	case *AppendEntriesRequest:
		r.appendEntries(rpc, cmd)
	default:
		r.logger.Printf("[ERR] raft: Expected heartbeat, got command: %#v", rpc.Command)
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "appendEntries"}, time.Now())

	resp := &AppendEntriesResponse{
		RPCHeader:      r.getRPCHeader(),
		Term:           r.getCurrentTerm(),
		LastLog:        r.getLastIndex(),
		Success:        false,
		NoRetryBackoff: false,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	if a.Term < r.getCurrentTerm() {
		return
	}

	if a.Term > r.getCurrentTerm() || r.getState() != Follower {

		r.setState(Follower)
		r.setCurrentTerm(a.Term)
		resp.Term = a.Term
	}

	r.setLeader(ServerAddress(r.trans.DecodePeer(a.Leader)))

	if a.PrevLogEntry > 0 {
		lastIdx, lastTerm := r.getLastEntry()

		var prevLogTerm uint64
		if a.PrevLogEntry == lastIdx {
			prevLogTerm = lastTerm

		} else {
			var prevLog Log
			if err := r.logs.GetLog(a.PrevLogEntry, &prevLog); err != nil {
				r.logger.Printf("[WARN] raft: Failed to get previous log: %d %v (last: %d)",
					a.PrevLogEntry, err, lastIdx)
				resp.NoRetryBackoff = true
				return
			}
			prevLogTerm = prevLog.Term
		}

		if a.PrevLogTerm != prevLogTerm {
			r.logger.Printf("[WARN] raft: Previous log term mis-match: ours: %d remote: %d",
				prevLogTerm, a.PrevLogTerm)
			resp.NoRetryBackoff = true
			return
		}
	}

	if len(a.Entries) > 0 {
		start := time.Now()

		lastLogIdx, _ := r.getLastLog()
		var newEntries []*Log
		for i, entry := range a.Entries {
			if entry.Index > lastLogIdx {
				newEntries = a.Entries[i:]
				break
			}
			var storeEntry Log
			if err := r.logs.GetLog(entry.Index, &storeEntry); err != nil {
				r.logger.Printf("[WARN] raft: Failed to get log entry %d: %v",
					entry.Index, err)
				return
			}
			if entry.Term != storeEntry.Term {
				r.logger.Printf("[WARN] raft: Clearing log suffix from %d to %d", entry.Index, lastLogIdx)
				if err := r.logs.DeleteRange(entry.Index, lastLogIdx); err != nil {
					r.logger.Printf("[ERR] raft: Failed to clear log suffix: %v", err)
					return
				}
				if entry.Index <= r.configurations.latestIndex {
					r.configurations.latest = r.configurations.committed
					r.configurations.latestIndex = r.configurations.committedIndex
				}
				newEntries = a.Entries[i:]
				break
			}
		}

		if n := len(newEntries); n > 0 {

			if err := r.logs.StoreLogs(newEntries); err != nil {
				r.logger.Printf("[ERR] raft: Failed to append to logs: %v", err)

				return
			}

			for _, newEntry := range newEntries {
				r.processConfigurationLogEntry(newEntry)
			}

			last := newEntries[n-1]
			r.setLastLog(last.Index, last.Term)
		}

		metrics.MeasureSince([]string{"raft", "rpc", "appendEntries", "storeLogs"}, start)
	}

	if a.LeaderCommitIndex > 0 && a.LeaderCommitIndex > r.getCommitIndex() {
		start := time.Now()
		idx := min(a.LeaderCommitIndex, r.getLastIndex())
		r.setCommitIndex(idx)
		if r.configurations.latestIndex <= idx {
			r.configurations.committed = r.configurations.latest
			r.configurations.committedIndex = r.configurations.latestIndex
		}
		r.processLogs(idx, nil)
		metrics.MeasureSince([]string{"raft", "rpc", "appendEntries", "processLogs"}, start)
	}

	resp.Success = true
	r.setLastContact()
	return
}

func (r *Raft) processConfigurationLogEntry(entry *Log) {
	if entry.Type == LogConfiguration {
		r.configurations.committed = r.configurations.latest
		r.configurations.committedIndex = r.configurations.latestIndex
		r.configurations.latest = decodeConfiguration(entry.Data)
		r.configurations.latestIndex = entry.Index
	} else if entry.Type == LogAddPeerDeprecated || entry.Type == LogRemovePeerDeprecated {
		r.configurations.committed = r.configurations.latest
		r.configurations.committedIndex = r.configurations.latestIndex
		r.configurations.latest = decodePeers(entry.Data, r.trans)
		r.configurations.latestIndex = entry.Index
	}
}

func (r *Raft) requestVote(rpc RPC, req *RequestVoteRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "requestVote"}, time.Now())
	r.observe(*req)

	resp := &RequestVoteResponse{
		RPCHeader: r.getRPCHeader(),
		Term:      r.getCurrentTerm(),
		Granted:   false,
	}
	var rpcErr error
	defer func() {
		rpc.Respond(resp, rpcErr)
	}()

	if r.protocolVersion < 2 {
		resp.Peers = encodePeers(r.configurations.latest, r.trans)
	}

	candidate := r.trans.DecodePeer(req.Candidate)
	if leader := r.Leader(); leader != "" && leader != candidate {
		r.logger.Printf("[WARN] raft: Rejecting vote request from %v since we have a leader: %v",
			candidate, leader)
		return
	}

	if req.Term < r.getCurrentTerm() {
		return
	}

	if req.Term > r.getCurrentTerm() {

		r.setState(Follower)
		r.setCurrentTerm(req.Term)
		resp.Term = req.Term
	}

	lastVoteTerm, err := r.stable.GetUint64(keyLastVoteTerm)
	if err != nil && err.Error() != "not found" {
		r.logger.Printf("[ERR] raft: Failed to get last vote term: %v", err)
		return
	}
	lastVoteCandBytes, err := r.stable.Get(keyLastVoteCand)
	if err != nil && err.Error() != "not found" {
		r.logger.Printf("[ERR] raft: Failed to get last vote candidate: %v", err)
		return
	}

	if lastVoteTerm == req.Term && lastVoteCandBytes != nil {
		r.logger.Printf("[INFO] raft: Duplicate RequestVote for same term: %d", req.Term)
		if bytes.Compare(lastVoteCandBytes, req.Candidate) == 0 {
			r.logger.Printf("[WARN] raft: Duplicate RequestVote from candidate: %s", req.Candidate)
			resp.Granted = true
		}
		return
	}

	lastIdx, lastTerm := r.getLastEntry()
	if lastTerm > req.LastLogTerm {
		r.logger.Printf("[WARN] raft: Rejecting vote request from %v since our last term is greater (%d, %d)",
			candidate, lastTerm, req.LastLogTerm)
		return
	}

	if lastTerm == req.LastLogTerm && lastIdx > req.LastLogIndex {
		r.logger.Printf("[WARN] raft: Rejecting vote request from %v since our last index is greater (%d, %d)",
			candidate, lastIdx, req.LastLogIndex)
		return
	}

	if err := r.persistVote(req.Term, req.Candidate); err != nil {
		r.logger.Printf("[ERR] raft: Failed to persist vote: %v", err)
		return
	}

	resp.Granted = true
	r.setLastContact()
	return
}

func (r *Raft) installSnapshot(rpc RPC, req *InstallSnapshotRequest) {
	defer metrics.MeasureSince([]string{"raft", "rpc", "installSnapshot"}, time.Now())

	resp := &InstallSnapshotResponse{
		Term:    r.getCurrentTerm(),
		Success: false,
	}
	var rpcErr error
	defer func() {
		io.Copy(ioutil.Discard, rpc.Reader) // ensure we always consume all the snapshot data from the stream [see issue #212]
		rpc.Respond(resp, rpcErr)
	}()

	if req.SnapshotVersion < SnapshotVersionMin ||
		req.SnapshotVersion > SnapshotVersionMax {
		rpcErr = fmt.Errorf("unsupported snapshot version %d", req.SnapshotVersion)
		return
	}

	if req.Term < r.getCurrentTerm() {
		r.logger.Printf("[INFO] raft: Ignoring installSnapshot request with older term of %d vs currentTerm %d", req.Term, r.getCurrentTerm())
		return
	}

	if req.Term > r.getCurrentTerm() {

		r.setState(Follower)
		r.setCurrentTerm(req.Term)
		resp.Term = req.Term
	}

	r.setLeader(ServerAddress(r.trans.DecodePeer(req.Leader)))

	var reqConfiguration Configuration
	var reqConfigurationIndex uint64
	if req.SnapshotVersion > 0 {
		reqConfiguration = decodeConfiguration(req.Configuration)
		reqConfigurationIndex = req.ConfigurationIndex
	} else {
		reqConfiguration = decodePeers(req.Peers, r.trans)
		reqConfigurationIndex = req.LastLogIndex
	}
	version := getSnapshotVersion(r.protocolVersion)
	sink, err := r.snapshots.Create(version, req.LastLogIndex, req.LastLogTerm,
		reqConfiguration, reqConfigurationIndex, r.trans)
	if err != nil {
		r.logger.Printf("[ERR] raft: Failed to create snapshot to install: %v", err)
		rpcErr = fmt.Errorf("failed to create snapshot: %v", err)
		return
	}

	n, err := io.Copy(sink, rpc.Reader)
	if err != nil {
		sink.Cancel()
		r.logger.Printf("[ERR] raft: Failed to copy snapshot: %v", err)
		rpcErr = err
		return
	}

	if n != req.Size {
		sink.Cancel()
		r.logger.Printf("[ERR] raft: Failed to receive whole snapshot: %d / %d", n, req.Size)
		rpcErr = fmt.Errorf("short read")
		return
	}

	if err := sink.Close(); err != nil {
		r.logger.Printf("[ERR] raft: Failed to finalize snapshot: %v", err)
		rpcErr = err
		return
	}
	r.logger.Printf("[INFO] raft: Copied %d bytes to local snapshot", n)

	future := &restoreFuture{ID: sink.ID()}
	future.init()
	select {
	case r.fsmMutateCh <- future:
	case <-r.shutdownCh:
		future.respond(ErrRaftShutdown)
		return
	}

	if err := future.Error(); err != nil {
		r.logger.Printf("[ERR] raft: Failed to restore snapshot: %v", err)
		rpcErr = err
		return
	}

	r.setLastApplied(req.LastLogIndex)

	r.setLastSnapshot(req.LastLogIndex, req.LastLogTerm)

	r.configurations.latest = reqConfiguration
	r.configurations.latestIndex = reqConfigurationIndex
	r.configurations.committed = reqConfiguration
	r.configurations.committedIndex = reqConfigurationIndex

	if err := r.compactLogs(req.LastLogIndex); err != nil {
		r.logger.Printf("[ERR] raft: Failed to compact logs: %v", err)
	}

	r.logger.Printf("[INFO] raft: Installed remote snapshot")
	resp.Success = true
	r.setLastContact()
	return
}

func (r *Raft) setLastContact() {
	r.lastContactLock.Lock()
	r.lastContact = time.Now()
	r.lastContactLock.Unlock()
}

type voteResult struct {
	RequestVoteResponse
	voterID ServerID
}

func (r *Raft) electSelf() <-chan *voteResult {

	respCh := make(chan *voteResult, len(r.configurations.latest.Servers))

	r.setCurrentTerm(r.getCurrentTerm() + 1)

	lastIdx, lastTerm := r.getLastEntry()
	req := &RequestVoteRequest{
		RPCHeader:    r.getRPCHeader(),
		Term:         r.getCurrentTerm(),
		Candidate:    r.trans.EncodePeer(r.localID, r.localAddr),
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	askPeer := func(peer Server) {
		r.goFunc(func() {
			defer metrics.MeasureSince([]string{"raft", "candidate", "electSelf"}, time.Now())
			resp := &voteResult{voterID: peer.ID}
			err := r.trans.RequestVote(peer.ID, peer.Address, req, &resp.RequestVoteResponse)
			if err != nil {
				r.logger.Printf("[ERR] raft: Failed to make RequestVote RPC to %v: %v", peer, err)
				resp.Term = req.Term
				resp.Granted = false
			}
			respCh <- resp
		})
	}

	for _, server := range r.configurations.latest.Servers {
		if server.Suffrage == Voter {
			if server.ID == r.localID {

				if err := r.persistVote(req.Term, req.Candidate); err != nil {
					r.logger.Printf("[ERR] raft: Failed to persist vote : %v", err)
					return nil
				}

				respCh <- &voteResult{
					RequestVoteResponse: RequestVoteResponse{
						RPCHeader: r.getRPCHeader(),
						Term:      req.Term,
						Granted:   true,
					},
					voterID: r.localID,
				}
			} else {
				askPeer(server)
			}
		}
	}

	return respCh
}

func (r *Raft) persistVote(term uint64, candidate []byte) error {
	if err := r.stable.SetUint64(keyLastVoteTerm, term); err != nil {
		return err
	}
	if err := r.stable.Set(keyLastVoteCand, candidate); err != nil {
		return err
	}
	return nil
}

func (r *Raft) setCurrentTerm(t uint64) {

	if err := r.stable.SetUint64(keyCurrentTerm, t); err != nil {
		panic(fmt.Errorf("failed to save current term: %v", err))
	}
	r.raftState.setCurrentTerm(t)
}

func (r *Raft) setState(state RaftState) {
	r.setLeader("")
	oldState := r.raftState.getState()
	r.raftState.setState(state)
	if oldState != state {
		r.observe(state)
	}
}
