package raft

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/armon/go-metrics"
)

var (
	ErrLeader = errors.New("node is the leader")

	ErrNotLeader = errors.New("node is not the leader")

	ErrLeadershipLost = errors.New("leadership lost while committing log")

	ErrAbortedByRestore = errors.New("snapshot restored while committing log")

	ErrRaftShutdown = errors.New("raft is already shutdown")

	ErrEnqueueTimeout = errors.New("timed out enqueuing operation")

	ErrNothingNewToSnapshot = errors.New("nothing new to snapshot")

	ErrUnsupportedProtocol = errors.New("operation not supported with current protocol version")

	ErrCantBootstrap = errors.New("bootstrap only works on new clusters")
)

type Raft struct {
	raftState

	protocolVersion ProtocolVersion

	applyCh chan *logFuture

	conf Config

	fsm FSM

	fsmMutateCh chan interface{}

	fsmSnapshotCh chan *reqSnapshotFuture

	lastContact     time.Time
	lastContactLock sync.RWMutex

	leader     ServerAddress
	leaderLock sync.RWMutex

	leaderCh chan bool

	leaderState leaderState

	localID ServerID

	localAddr ServerAddress

	logger *log.Logger

	logs LogStore

	configurationChangeCh chan *configurationChangeFuture

	configurations configurations

	rpcCh <-chan RPC

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	snapshots SnapshotStore

	userSnapshotCh chan *userSnapshotFuture

	userRestoreCh chan *userRestoreFuture

	stable StableStore

	trans Transport

	verifyCh chan *verifyFuture

	configurationsCh chan *configurationsFuture

	bootstrapCh chan *bootstrapFuture

	observersLock sync.RWMutex
	observers     map[uint64]*Observer
}

//
func BootstrapCluster(conf *Config, logs LogStore, stable StableStore,
	snaps SnapshotStore, trans Transport, configuration Configuration) error {

	if err := ValidateConfig(conf); err != nil {
		return err
	}

	if err := checkConfiguration(configuration); err != nil {
		return err
	}

	hasState, err := HasExistingState(logs, stable, snaps)
	if err != nil {
		return fmt.Errorf("failed to check for existing state: %v", err)
	}
	if hasState {
		return ErrCantBootstrap
	}

	if err := stable.SetUint64(keyCurrentTerm, 1); err != nil {
		return fmt.Errorf("failed to save current term: %v", err)
	}

	entry := &Log{
		Index: 1,
		Term:  1,
	}
	if conf.ProtocolVersion < 3 {
		entry.Type = LogRemovePeerDeprecated
		entry.Data = encodePeers(configuration, trans)
	} else {
		entry.Type = LogConfiguration
		entry.Data = encodeConfiguration(configuration)
	}
	if err := logs.StoreLog(entry); err != nil {
		return fmt.Errorf("failed to append configuration entry to log: %v", err)
	}

	return nil
}

//
//
//
func RecoverCluster(conf *Config, fsm FSM, logs LogStore, stable StableStore,
	snaps SnapshotStore, trans Transport, configuration Configuration) error {

	if err := ValidateConfig(conf); err != nil {
		return err
	}

	if err := checkConfiguration(configuration); err != nil {
		return err
	}

	hasState, err := HasExistingState(logs, stable, snaps)
	if err != nil {
		return fmt.Errorf("failed to check for existing state: %v", err)
	}
	if !hasState {
		return fmt.Errorf("refused to recover cluster with no initial state, this is probably an operator error")
	}

	var snapshotIndex uint64
	var snapshotTerm uint64
	snapshots, err := snaps.List()
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %v", err)
	}
	for _, snapshot := range snapshots {
		_, source, err := snaps.Open(snapshot.ID)
		if err != nil {

			continue
		}
		defer source.Close()

		if err := fsm.Restore(source); err != nil {

			continue
		}

		snapshotIndex = snapshot.Index
		snapshotTerm = snapshot.Term
		break
	}
	if len(snapshots) > 0 && (snapshotIndex == 0 || snapshotTerm == 0) {
		return fmt.Errorf("failed to restore any of the available snapshots")
	}

	lastIndex := snapshotIndex
	lastTerm := snapshotTerm

	lastLogIndex, err := logs.LastIndex()
	if err != nil {
		return fmt.Errorf("failed to find last log: %v", err)
	}
	for index := snapshotIndex + 1; index <= lastLogIndex; index++ {
		var entry Log
		if err := logs.GetLog(index, &entry); err != nil {
			return fmt.Errorf("failed to get log at index %d: %v", index, err)
		}
		if entry.Type == LogCommand {
			_ = fsm.Apply(&entry)
		}
		lastIndex = entry.Index
		lastTerm = entry.Term
	}

	snapshot, err := fsm.Snapshot()
	if err != nil {
		return fmt.Errorf("failed to snapshot FSM: %v", err)
	}
	version := getSnapshotVersion(conf.ProtocolVersion)
	sink, err := snaps.Create(version, lastIndex, lastTerm, configuration, 1, trans)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}
	if err := snapshot.Persist(sink); err != nil {
		return fmt.Errorf("failed to persist snapshot: %v", err)
	}
	if err := sink.Close(); err != nil {
		return fmt.Errorf("failed to finalize snapshot: %v", err)
	}

	firstLogIndex, err := logs.FirstIndex()
	if err != nil {
		return fmt.Errorf("failed to get first log index: %v", err)
	}
	if err := logs.DeleteRange(firstLogIndex, lastLogIndex); err != nil {
		return fmt.Errorf("log compaction failed: %v", err)
	}

	return nil
}

func HasExistingState(logs LogStore, stable StableStore, snaps SnapshotStore) (bool, error) {

	currentTerm, err := stable.GetUint64(keyCurrentTerm)
	if err == nil {
		if currentTerm > 0 {
			return true, nil
		}
	} else {
		if err.Error() != "not found" {
			return false, fmt.Errorf("failed to read current term: %v", err)
		}
	}

	lastIndex, err := logs.LastIndex()
	if err != nil {
		return false, fmt.Errorf("failed to get last log index: %v", err)
	}
	if lastIndex > 0 {
		return true, nil
	}

	snapshots, err := snaps.List()
	if err != nil {
		return false, fmt.Errorf("failed to list snapshots: %v", err)
	}
	if len(snapshots) > 0 {
		return true, nil
	}

	return false, nil
}

func NewRaft(conf *Config, fsm FSM, logs LogStore, stable StableStore, snaps SnapshotStore, trans Transport) (*Raft, error) {

	if err := ValidateConfig(conf); err != nil {
		return nil, err
	}

	var logger *log.Logger
	if conf.Logger != nil {
		logger = conf.Logger
	} else {
		if conf.LogOutput == nil {
			conf.LogOutput = os.Stderr
		}
		logger = log.New(conf.LogOutput, "", log.LstdFlags)
	}

	currentTerm, err := stable.GetUint64(keyCurrentTerm)
	if err != nil && err.Error() != "not found" {
		return nil, fmt.Errorf("failed to load current term: %v", err)
	}

	lastIndex, err := logs.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to find last log: %v", err)
	}

	var lastLog Log
	if lastIndex > 0 {
		if err = logs.GetLog(lastIndex, &lastLog); err != nil {
			return nil, fmt.Errorf("failed to get last log at index %d: %v", lastIndex, err)
		}
	}

	protocolVersion := conf.ProtocolVersion
	localAddr := ServerAddress(trans.LocalAddr())
	localID := conf.LocalID

	if protocolVersion < 3 && string(localID) != string(localAddr) {
		return nil, fmt.Errorf("when running with ProtocolVersion < 3, LocalID must be set to the network address")
	}

	r := &Raft{
		protocolVersion: protocolVersion,
		applyCh:         make(chan *logFuture),
		conf:            *conf,
		fsm:             fsm,
		fsmMutateCh:     make(chan interface{}, 128),
		fsmSnapshotCh:   make(chan *reqSnapshotFuture),
		leaderCh:        make(chan bool),
		localID:         localID,
		localAddr:       localAddr,
		logger:          logger,
		logs:            logs,
		configurationChangeCh: make(chan *configurationChangeFuture),
		configurations:        configurations{},
		rpcCh:                 trans.Consumer(),
		snapshots:             snaps,
		userSnapshotCh:        make(chan *userSnapshotFuture),
		userRestoreCh:         make(chan *userRestoreFuture),
		shutdownCh:            make(chan struct{}),
		stable:                stable,
		trans:                 trans,
		verifyCh:              make(chan *verifyFuture, 64),
		configurationsCh:      make(chan *configurationsFuture, 8),
		bootstrapCh:           make(chan *bootstrapFuture),
		observers:             make(map[uint64]*Observer),
	}

	r.setState(Follower)

	if conf.StartAsLeader {
		r.setState(Leader)
		r.setLeader(r.localAddr)
	}

	r.setCurrentTerm(currentTerm)
	r.setLastLog(lastLog.Index, lastLog.Term)

	if err := r.restoreSnapshot(); err != nil {
		return nil, err
	}

	snapshotIndex, _ := r.getLastSnapshot()
	for index := snapshotIndex + 1; index <= lastLog.Index; index++ {
		var entry Log
		if err := r.logs.GetLog(index, &entry); err != nil {
			r.logger.Printf("[ERR] raft: Failed to get log at %d: %v", index, err)
			panic(err)
		}
		r.processConfigurationLogEntry(&entry)
	}

	r.logger.Printf("[INFO] raft: Initial configuration (index=%d): %+v",
		r.configurations.latestIndex, r.configurations.latest.Servers)

	trans.SetHeartbeatHandler(r.processHeartbeat)

	r.goFunc(r.run)
	r.goFunc(r.runFSM)
	r.goFunc(r.runSnapshots)
	return r, nil
}

func (r *Raft) restoreSnapshot() error {
	snapshots, err := r.snapshots.List()
	if err != nil {
		r.logger.Printf("[ERR] raft: Failed to list snapshots: %v", err)
		return err
	}

	for _, snapshot := range snapshots {
		_, source, err := r.snapshots.Open(snapshot.ID)
		if err != nil {
			r.logger.Printf("[ERR] raft: Failed to open snapshot %v: %v", snapshot.ID, err)
			continue
		}
		defer source.Close()

		if err := r.fsm.Restore(source); err != nil {
			r.logger.Printf("[ERR] raft: Failed to restore snapshot %v: %v", snapshot.ID, err)
			continue
		}

		r.logger.Printf("[INFO] raft: Restored from snapshot %v", snapshot.ID)

		r.setLastApplied(snapshot.Index)

		r.setLastSnapshot(snapshot.Index, snapshot.Term)

		if snapshot.Version > 0 {
			r.configurations.committed = snapshot.Configuration
			r.configurations.committedIndex = snapshot.ConfigurationIndex
			r.configurations.latest = snapshot.Configuration
			r.configurations.latestIndex = snapshot.ConfigurationIndex
		} else {
			configuration := decodePeers(snapshot.Peers, r.trans)
			r.configurations.committed = configuration
			r.configurations.committedIndex = snapshot.Index
			r.configurations.latest = configuration
			r.configurations.latestIndex = snapshot.Index
		}

		return nil
	}

	if len(snapshots) > 0 {
		return fmt.Errorf("failed to load any existing snapshots")
	}
	return nil
}

func (r *Raft) BootstrapCluster(configuration Configuration) Future {
	bootstrapReq := &bootstrapFuture{}
	bootstrapReq.init()
	bootstrapReq.configuration = configuration
	select {
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.bootstrapCh <- bootstrapReq:
		return bootstrapReq
	}
}

func (r *Raft) Leader() ServerAddress {
	r.leaderLock.RLock()
	leader := r.leader
	r.leaderLock.RUnlock()
	return leader
}

func (r *Raft) Apply(cmd []byte, timeout time.Duration) ApplyFuture {
	metrics.IncrCounter([]string{"raft", "apply"}, 1)
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	logFuture := &logFuture{
		log: Log{
			Type: LogCommand,
			Data: cmd,
		},
	}
	logFuture.init()

	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.applyCh <- logFuture:
		return logFuture
	}
}

func (r *Raft) Barrier(timeout time.Duration) Future {
	metrics.IncrCounter([]string{"raft", "barrier"}, 1)
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	logFuture := &logFuture{
		log: Log{
			Type: LogBarrier,
		},
	}
	logFuture.init()

	select {
	case <-timer:
		return errorFuture{ErrEnqueueTimeout}
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.applyCh <- logFuture:
		return logFuture
	}
}

func (r *Raft) VerifyLeader() Future {
	metrics.IncrCounter([]string{"raft", "verify_leader"}, 1)
	verifyFuture := &verifyFuture{}
	verifyFuture.init()
	select {
	case <-r.shutdownCh:
		return errorFuture{ErrRaftShutdown}
	case r.verifyCh <- verifyFuture:
		return verifyFuture
	}
}

func (r *Raft) GetConfiguration() ConfigurationFuture {
	configReq := &configurationsFuture{}
	configReq.init()
	select {
	case <-r.shutdownCh:
		configReq.respond(ErrRaftShutdown)
		return configReq
	case r.configurationsCh <- configReq:
		return configReq
	}
}

func (r *Raft) AddPeer(peer ServerAddress) Future {
	if r.protocolVersion > 2 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestConfigChange(configurationChangeRequest{
		command:       AddStaging,
		serverID:      ServerID(peer),
		serverAddress: peer,
		prevIndex:     0,
	}, 0)
}

func (r *Raft) RemovePeer(peer ServerAddress) Future {
	if r.protocolVersion > 2 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestConfigChange(configurationChangeRequest{
		command:   RemoveServer,
		serverID:  ServerID(peer),
		prevIndex: 0,
	}, 0)
}

func (r *Raft) AddVoter(id ServerID, address ServerAddress, prevIndex uint64, timeout time.Duration) IndexFuture {
	if r.protocolVersion < 2 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestConfigChange(configurationChangeRequest{
		command:       AddStaging,
		serverID:      id,
		serverAddress: address,
		prevIndex:     prevIndex,
	}, timeout)
}

func (r *Raft) AddNonvoter(id ServerID, address ServerAddress, prevIndex uint64, timeout time.Duration) IndexFuture {
	if r.protocolVersion < 3 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestConfigChange(configurationChangeRequest{
		command:       AddNonvoter,
		serverID:      id,
		serverAddress: address,
		prevIndex:     prevIndex,
	}, timeout)
}

func (r *Raft) RemoveServer(id ServerID, prevIndex uint64, timeout time.Duration) IndexFuture {
	if r.protocolVersion < 2 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestConfigChange(configurationChangeRequest{
		command:   RemoveServer,
		serverID:  id,
		prevIndex: prevIndex,
	}, timeout)
}

func (r *Raft) DemoteVoter(id ServerID, prevIndex uint64, timeout time.Duration) IndexFuture {
	if r.protocolVersion < 3 {
		return errorFuture{ErrUnsupportedProtocol}
	}

	return r.requestConfigChange(configurationChangeRequest{
		command:   DemoteVoter,
		serverID:  id,
		prevIndex: prevIndex,
	}, timeout)
}

func (r *Raft) Shutdown() Future {
	r.shutdownLock.Lock()
	defer r.shutdownLock.Unlock()

	if !r.shutdown {
		close(r.shutdownCh)
		r.shutdown = true
		r.setState(Shutdown)
		return &shutdownFuture{r}
	}

	return &shutdownFuture{nil}
}

func (r *Raft) Snapshot() SnapshotFuture {
	future := &userSnapshotFuture{}
	future.init()
	select {
	case r.userSnapshotCh <- future:
		return future
	case <-r.shutdownCh:
		future.respond(ErrRaftShutdown)
		return future
	}
}

//
func (r *Raft) Restore(meta *SnapshotMeta, reader io.Reader, timeout time.Duration) error {
	metrics.IncrCounter([]string{"raft", "restore"}, 1)
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(timeout)
	}

	restore := &userRestoreFuture{
		meta:   meta,
		reader: reader,
	}
	restore.init()
	select {
	case <-timer:
		return ErrEnqueueTimeout
	case <-r.shutdownCh:
		return ErrRaftShutdown
	case r.userRestoreCh <- restore:

		if err := restore.Error(); err != nil {
			return err
		}
	}

	noop := &logFuture{
		log: Log{
			Type: LogNoop,
		},
	}
	noop.init()
	select {
	case <-timer:
		return ErrEnqueueTimeout
	case <-r.shutdownCh:
		return ErrRaftShutdown
	case r.applyCh <- noop:
		return noop.Error()
	}
}

func (r *Raft) State() RaftState {
	return r.getState()
}

func (r *Raft) LeaderCh() <-chan bool {
	return r.leaderCh
}

func (r *Raft) String() string {
	return fmt.Sprintf("Node at %s [%v]", r.localAddr, r.getState())
}

func (r *Raft) LastContact() time.Time {
	r.lastContactLock.RLock()
	last := r.lastContact
	r.lastContactLock.RUnlock()
	return last
}

//
//
//
//
//
//
func (r *Raft) Stats() map[string]string {
	toString := func(v uint64) string {
		return strconv.FormatUint(v, 10)
	}
	lastLogIndex, lastLogTerm := r.getLastLog()
	lastSnapIndex, lastSnapTerm := r.getLastSnapshot()
	s := map[string]string{
		"state":                r.getState().String(),
		"term":                 toString(r.getCurrentTerm()),
		"last_log_index":       toString(lastLogIndex),
		"last_log_term":        toString(lastLogTerm),
		"commit_index":         toString(r.getCommitIndex()),
		"applied_index":        toString(r.getLastApplied()),
		"fsm_pending":          toString(uint64(len(r.fsmMutateCh))),
		"last_snapshot_index":  toString(lastSnapIndex),
		"last_snapshot_term":   toString(lastSnapTerm),
		"protocol_version":     toString(uint64(r.protocolVersion)),
		"protocol_version_min": toString(uint64(ProtocolVersionMin)),
		"protocol_version_max": toString(uint64(ProtocolVersionMax)),
		"snapshot_version_min": toString(uint64(SnapshotVersionMin)),
		"snapshot_version_max": toString(uint64(SnapshotVersionMax)),
	}

	future := r.GetConfiguration()
	if err := future.Error(); err != nil {
		r.logger.Printf("[WARN] raft: could not get configuration for Stats: %v", err)
	} else {
		configuration := future.Configuration()
		s["latest_configuration_index"] = toString(future.Index())
		s["latest_configuration"] = fmt.Sprintf("%+v", configuration.Servers)

		hasUs := false
		numPeers := 0
		for _, server := range configuration.Servers {
			if server.Suffrage == Voter {
				if server.ID == r.localID {
					hasUs = true
				} else {
					numPeers++
				}
			}
		}
		if !hasUs {
			numPeers = 0
		}
		s["num_peers"] = toString(uint64(numPeers))
	}

	last := r.LastContact()
	if r.getState() == Leader {
		s["last_contact"] = "0"
	} else if last.IsZero() {
		s["last_contact"] = "never"
	} else {
		s["last_contact"] = fmt.Sprintf("%v", time.Now().Sub(last))
	}
	return s
}

func (r *Raft) LastIndex() uint64 {
	return r.getLastIndex()
}

func (r *Raft) AppliedIndex() uint64 {
	return r.getLastApplied()
}
