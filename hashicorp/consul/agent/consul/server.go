package consul

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/as/consulrun/hashicorp/consul/acl"
	"github.com/as/consulrun/hashicorp/consul/agent/consul/autopilot"
	"github.com/as/consulrun/hashicorp/consul/agent/consul/fsm"
	"github.com/as/consulrun/hashicorp/consul/agent/consul/state"
	"github.com/as/consulrun/hashicorp/consul/agent/metadata"
	"github.com/as/consulrun/hashicorp/consul/agent/pool"
	"github.com/as/consulrun/hashicorp/consul/agent/router"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/agent/token"
	"github.com/as/consulrun/hashicorp/consul/lib"
	"github.com/as/consulrun/hashicorp/consul/sentinel"
	"github.com/as/consulrun/hashicorp/consul/tlsutil"
	"github.com/as/consulrun/hashicorp/consul/types"
	"github.com/as/consulrun/hashicorp/raft"
	raftboltdb "github.com/as/consulrun/hashicorp/raft-boltdb"
	"github.com/as/consulrun/hashicorp/serf/serf"
)

const (
	ProtocolVersionMin uint8 = 2

	ProtocolVersion2Compatible = 2

	ProtocolVersionMax = 3
)

const (
	serfLANSnapshot   = "serf/local.snapshot"
	serfWANSnapshot   = "serf/remote.snapshot"
	raftState         = "raft/"
	snapshotsRetained = 2

	serverRPCCache = 2 * time.Minute

	serverMaxStreams = 64

	raftLogCacheSize = 512

	raftRemoveGracePeriod = 5 * time.Second
)

var (
	ErrWANFederationDisabled = fmt.Errorf("WAN Federation is disabled")
)

type Server struct {
	sentinel sentinel.Evaluator

	aclAuthCache *acl.Cache

	aclCache *aclCache

	autopilot *autopilot.Autopilot

	autopilotWaitGroup sync.WaitGroup

	config *Config

	tokens *token.Store

	connPool *pool.ConnPool

	eventChLAN chan serf.Event

	eventChWAN chan serf.Event

	fsm *fsm.FSM

	logger *log.Logger

	raft          *raft.Raft
	raftLayer     *RaftLayer
	raftStore     *raftboltdb.BoltStore
	raftTransport *raft.NetworkTransport
	raftInmem     *raft.InmemStore

	raftNotifyCh <-chan bool

	reconcileCh chan serf.Member

	readyForConsistentReads int32

	leaveCh chan struct{}

	router *router.Router

	Listener  net.Listener
	rpcServer *rpc.Server

	rpcTLS *tls.Config

	serfLAN *serf.Serf

	segmentLAN map[string]*serf.Serf

	serfWAN *serf.Serf

	serverLookup *ServerLookup

	floodLock sync.RWMutex
	floodCh   []chan struct{}

	sessionTimers *SessionTimers

	statsFetcher *StatsFetcher

	reassertLeaderCh chan chan error

	tombstoneGC *state.TombstoneGC

	aclReplicationStatus     structs.ACLReplicationStatus
	aclReplicationStatusLock sync.RWMutex

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

func NewServer(config *Config) (*Server, error) {
	return NewServerLogger(config, nil, new(token.Store))
}

func NewServerLogger(config *Config, logger *log.Logger, tokens *token.Store) (*Server, error) {

	if err := config.CheckProtocolVersion(); err != nil {
		return nil, err
	}

	if config.DataDir == "" && !config.DevMode {
		return nil, fmt.Errorf("Config must provide a DataDir")
	}

	if err := config.CheckACL(); err != nil {
		return nil, err
	}

	if config.LogOutput == nil {
		config.LogOutput = os.Stderr
	}
	if logger == nil {
		logger = log.New(config.LogOutput, "", log.LstdFlags)
	}

	if config.CAFile != "" || config.CAPath != "" {
		config.UseTLS = true
	}

	tlsConf := config.tlsConfig()
	tlsWrap, err := tlsConf.OutgoingTLSWrapper()
	if err != nil {
		return nil, err
	}

	incomingTLS, err := tlsConf.IncomingTLSConfig()
	if err != nil {
		return nil, err
	}

	gc, err := state.NewTombstoneGC(config.TombstoneTTL, config.TombstoneTTLGranularity)
	if err != nil {
		return nil, err
	}

	shutdownCh := make(chan struct{})

	connPool := &pool.ConnPool{
		SrcAddr:    config.RPCSrcAddr,
		LogOutput:  config.LogOutput,
		MaxTime:    serverRPCCache,
		MaxStreams: serverMaxStreams,
		TLSWrapper: tlsWrap,
		ForceTLS:   config.VerifyOutgoing,
	}

	s := &Server{
		config:           config,
		tokens:           tokens,
		connPool:         connPool,
		eventChLAN:       make(chan serf.Event, 256),
		eventChWAN:       make(chan serf.Event, 256),
		logger:           logger,
		leaveCh:          make(chan struct{}),
		reconcileCh:      make(chan serf.Member, 32),
		router:           router.NewRouter(logger, config.Datacenter),
		rpcServer:        rpc.NewServer(),
		rpcTLS:           incomingTLS,
		reassertLeaderCh: make(chan chan error),
		segmentLAN:       make(map[string]*serf.Serf, len(config.Segments)),
		sessionTimers:    NewSessionTimers(),
		tombstoneGC:      gc,
		serverLookup:     NewServerLookup(),
		shutdownCh:       shutdownCh,
	}

	s.statsFetcher = NewStatsFetcher(logger, s.connPool, s.config.Datacenter)

	s.sentinel = sentinel.New(logger)
	s.aclAuthCache, err = acl.NewCache(aclCacheSize, s.aclLocalFault, s.sentinel)
	if err != nil {
		s.Shutdown()
		return nil, fmt.Errorf("Failed to create authoritative ACL cache: %v", err)
	}

	var local acl.FaultFunc
	if s.IsACLReplicationEnabled() {
		local = s.aclLocalFault
	}
	if s.aclCache, err = newACLCache(config, logger, s.RPC, local, s.sentinel); err != nil {
		s.Shutdown()
		return nil, fmt.Errorf("Failed to create non-authoritative ACL cache: %v", err)
	}

	if err := s.setupRPC(tlsWrap); err != nil {
		s.Shutdown()
		return nil, fmt.Errorf("Failed to start RPC layer: %v", err)
	}

	segmentListeners, err := s.setupSegmentRPC()
	if err != nil {
		s.Shutdown()
		return nil, fmt.Errorf("Failed to start segment RPC layer: %v", err)
	}

	if err := s.setupRaft(); err != nil {
		s.Shutdown()
		return nil, fmt.Errorf("Failed to start Raft: %v", err)
	}

	//

	serfBindPortWAN := -1
	if config.SerfWANConfig != nil {
		serfBindPortWAN = config.SerfWANConfig.MemberlistConfig.BindPort
		s.serfWAN, err = s.setupSerf(config.SerfWANConfig, s.eventChWAN, serfWANSnapshot, true, serfBindPortWAN, "", s.Listener)
		if err != nil {
			s.Shutdown()
			return nil, fmt.Errorf("Failed to start WAN Serf: %v", err)
		}

		if serfBindPortWAN == 0 {
			serfBindPortWAN = config.SerfWANConfig.MemberlistConfig.BindPort
			if serfBindPortWAN == 0 {
				return nil, fmt.Errorf("Failed to get dynamic bind port for WAN Serf")
			}
			s.logger.Printf("[INFO] agent: Serf WAN TCP bound to port %d", serfBindPortWAN)
		}
	}

	if err := s.setupSegments(config, serfBindPortWAN, segmentListeners); err != nil {
		s.Shutdown()
		return nil, fmt.Errorf("Failed to setup network segments: %v", err)
	}

	s.serfLAN, err = s.setupSerf(config.SerfLANConfig, s.eventChLAN, serfLANSnapshot, false, serfBindPortWAN, "", s.Listener)
	if err != nil {
		s.Shutdown()
		return nil, fmt.Errorf("Failed to start LAN Serf: %v", err)
	}
	go s.lanEventHandler()

	s.floodSegments(config)

	if s.serfWAN != nil {
		if err := s.router.AddArea(types.AreaWAN, s.serfWAN, s.connPool, s.config.VerifyOutgoing); err != nil {
			s.Shutdown()
			return nil, fmt.Errorf("Failed to add WAN serf route: %v", err)
		}
		go router.HandleSerfEvents(s.logger, s.router, types.AreaWAN, s.serfWAN.ShutdownCh(), s.eventChWAN)

		portFn := func(s *metadata.Server) (int, bool) {
			if s.WanJoinPort > 0 {
				return s.WanJoinPort, true
			}
			return 0, false
		}
		go s.Flood(nil, portFn, s.serfWAN)
	}

	go s.monitorLeadership()

	if s.IsACLReplicationEnabled() {
		go s.runACLReplication()
	}

	go s.listen(s.Listener)

	for _, listener := range segmentListeners {
		go s.listen(listener)
	}

	go s.sessionStats()

	s.initAutopilot(config)

	return s, nil
}

func (s *Server) setupRaft() error {

	defer func() {
		if s.raft == nil && s.raftStore != nil {
			if err := s.raftStore.Close(); err != nil {
				s.logger.Printf("[ERR] consul: failed to close Raft store: %v", err)
			}
		}
	}()

	var err error
	s.fsm, err = fsm.New(s.tombstoneGC, s.config.LogOutput)
	if err != nil {
		return err
	}

	var serverAddressProvider raft.ServerAddressProvider = nil
	if s.config.RaftConfig.ProtocolVersion >= 3 { //ServerAddressProvider needs server ids to work correctly, which is only supported in protocol version 3 or higher
		serverAddressProvider = s.serverLookup
	}

	transConfig := &raft.NetworkTransportConfig{
		Stream:                s.raftLayer,
		MaxPool:               3,
		Timeout:               10 * time.Second,
		ServerAddressProvider: serverAddressProvider,
	}

	trans := raft.NewNetworkTransportWithConfig(transConfig)
	s.raftTransport = trans

	s.config.RaftConfig.LogOutput = s.config.LogOutput
	s.config.RaftConfig.Logger = s.logger

	s.config.RaftConfig.LocalID = raft.ServerID(trans.LocalAddr())
	if s.config.RaftConfig.ProtocolVersion >= 3 {
		s.config.RaftConfig.LocalID = raft.ServerID(s.config.NodeID)
	}

	var log raft.LogStore
	var stable raft.StableStore
	var snap raft.SnapshotStore
	if s.config.DevMode {
		store := raft.NewInmemStore()
		s.raftInmem = store
		stable = store
		log = store
		snap = raft.NewInmemSnapshotStore()
	} else {

		path := filepath.Join(s.config.DataDir, raftState)
		if err := lib.EnsurePath(path, true); err != nil {
			return err
		}

		store, err := raftboltdb.NewBoltStore(filepath.Join(path, "raft.db"))
		if err != nil {
			return err
		}
		s.raftStore = store
		stable = store

		cacheStore, err := raft.NewLogCache(raftLogCacheSize, store)
		if err != nil {
			return err
		}
		log = cacheStore

		snapshots, err := raft.NewFileSnapshotStore(path, snapshotsRetained, s.config.LogOutput)
		if err != nil {
			return err
		}
		snap = snapshots

		peersFile := filepath.Join(path, "peers.json")
		peersInfoFile := filepath.Join(path, "peers.info")
		if _, err := os.Stat(peersInfoFile); os.IsNotExist(err) {
			if err := ioutil.WriteFile(peersInfoFile, []byte(peersInfoContent), 0755); err != nil {
				return fmt.Errorf("failed to write peers.info file: %v", err)
			}

			if _, err := os.Stat(peersFile); err == nil {
				if err := os.Remove(peersFile); err != nil {
					return fmt.Errorf("failed to delete peers.json, please delete manually (see peers.info for details): %v", err)
				}
				s.logger.Printf("[INFO] consul: deleted peers.json file (see peers.info for details)")
			}
		} else if _, err := os.Stat(peersFile); err == nil {
			s.logger.Printf("[INFO] consul: found peers.json file, recovering Raft configuration...")

			var configuration raft.Configuration
			if s.config.RaftConfig.ProtocolVersion < 3 {
				configuration, err = raft.ReadPeersJSON(peersFile)
			} else {
				configuration, err = raft.ReadConfigJSON(peersFile)
			}
			if err != nil {
				return fmt.Errorf("recovery failed to parse peers.json: %v", err)
			}

			tmpFsm, err := fsm.New(s.tombstoneGC, s.config.LogOutput)
			if err != nil {
				return fmt.Errorf("recovery failed to make temp FSM: %v", err)
			}
			if err := raft.RecoverCluster(s.config.RaftConfig, tmpFsm,
				log, stable, snap, trans, configuration); err != nil {
				return fmt.Errorf("recovery failed: %v", err)
			}

			if err := os.Remove(peersFile); err != nil {
				return fmt.Errorf("recovery failed to delete peers.json, please delete manually (see peers.info for details): %v", err)
			}
			s.logger.Printf("[INFO] consul: deleted peers.json file after successful recovery")
		}
	}

	if s.config.Bootstrap || s.config.DevMode {
		hasState, err := raft.HasExistingState(log, stable, snap)
		if err != nil {
			return err
		}
		if !hasState {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      s.config.RaftConfig.LocalID,
						Address: trans.LocalAddr(),
					},
				},
			}
			if err := raft.BootstrapCluster(s.config.RaftConfig,
				log, stable, snap, trans, configuration); err != nil {
				return err
			}
		}
	}

	raftNotifyCh := make(chan bool, 1)
	s.config.RaftConfig.NotifyCh = raftNotifyCh
	s.raftNotifyCh = raftNotifyCh

	s.raft, err = raft.NewRaft(s.config.RaftConfig, s.fsm, log, stable, snap, trans)
	if err != nil {
		return err
	}
	return nil
}

type factory func(s *Server) interface{}

var endpoints []factory

func registerEndpoint(fn factory) {
	endpoints = append(endpoints, fn)
}

func (s *Server) setupRPC(tlsWrap tlsutil.DCWrapper) error {
	for _, fn := range endpoints {
		s.rpcServer.Register(fn(s))
	}

	ln, err := net.ListenTCP("tcp", s.config.RPCAddr)
	if err != nil {
		return err
	}
	s.Listener = ln
	if s.config.NotifyListen != nil {
		s.config.NotifyListen()
	}

	if s.config.RPCAdvertise == nil {
		s.config.RPCAdvertise = ln.Addr().(*net.TCPAddr)
	}

	if s.config.RPCAdvertise.IP.IsUnspecified() {
		ln.Close()
		return fmt.Errorf("RPC advertise address is not advertisable: %v", s.config.RPCAdvertise)
	}

	wrapper := tlsutil.SpecificDC(s.config.Datacenter, tlsWrap)

	tlsFunc := func(address raft.ServerAddress) bool {
		if s.config.VerifyOutgoing {
			return true
		}

		server := s.serverLookup.Server(address)

		if server == nil {
			return false
		}

		return server.UseTLS
	}
	s.raftLayer = NewRaftLayer(s.config.RPCSrcAddr, s.config.RPCAdvertise, wrapper, tlsFunc)
	return nil
}

func (s *Server) Shutdown() error {
	s.logger.Printf("[INFO] consul: shutting down server")
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	if s.shutdown {
		return nil
	}

	s.shutdown = true
	close(s.shutdownCh)

	if s.serfLAN != nil {
		s.serfLAN.Shutdown()
	}

	if s.serfWAN != nil {
		s.serfWAN.Shutdown()
		if err := s.router.RemoveArea(types.AreaWAN); err != nil {
			s.logger.Printf("[WARN] consul: error removing WAN area: %v", err)
		}
	}
	s.router.Shutdown()

	if s.raft != nil {
		s.raftTransport.Close()
		s.raftLayer.Close()
		future := s.raft.Shutdown()
		if err := future.Error(); err != nil {
			s.logger.Printf("[WARN] consul: error shutting down raft: %s", err)
		}
		if s.raftStore != nil {
			s.raftStore.Close()
		}
	}

	if s.Listener != nil {
		s.Listener.Close()
	}

	s.connPool.Shutdown()

	return nil
}

func (s *Server) Leave() error {
	s.logger.Printf("[INFO] consul: server starting leave")

	numPeers, err := s.numPeers()
	if err != nil {
		s.logger.Printf("[ERR] consul: failed to check raft peers: %v", err)
		return err
	}

	addr := s.raftTransport.LocalAddr()

	isLeader := s.IsLeader()
	if isLeader && numPeers > 1 {
		minRaftProtocol, err := s.autopilot.MinRaftProtocol()
		if err != nil {
			return err
		}

		if minRaftProtocol >= 2 && s.config.RaftConfig.ProtocolVersion >= 3 {
			future := s.raft.RemoveServer(raft.ServerID(s.config.NodeID), 0, 0)
			if err := future.Error(); err != nil {
				s.logger.Printf("[ERR] consul: failed to remove ourself as raft peer: %v", err)
			}
		} else {
			future := s.raft.RemovePeer(addr)
			if err := future.Error(); err != nil {
				s.logger.Printf("[ERR] consul: failed to remove ourself as raft peer: %v", err)
			}
		}
	}

	if s.serfWAN != nil {
		if err := s.serfWAN.Leave(); err != nil {
			s.logger.Printf("[ERR] consul: failed to leave WAN Serf cluster: %v", err)
		}
	}

	if s.serfLAN != nil {
		if err := s.serfLAN.Leave(); err != nil {
			s.logger.Printf("[ERR] consul: failed to leave LAN Serf cluster: %v", err)
		}
	}

	s.logger.Printf("[INFO] consul: Waiting %s to drain RPC traffic", s.config.LeaveDrainTime)
	close(s.leaveCh)
	time.Sleep(s.config.LeaveDrainTime)

	if !isLeader {
		left := false
		limit := time.Now().Add(raftRemoveGracePeriod)
		for !left && time.Now().Before(limit) {

			time.Sleep(50 * time.Millisecond)

			future := s.raft.GetConfiguration()
			if err := future.Error(); err != nil {
				s.logger.Printf("[ERR] consul: failed to get raft configuration: %v", err)
				break
			}

			left = true
			for _, server := range future.Configuration().Servers {
				if server.Address == addr {
					left = false
					break
				}
			}
		}

		if !left {
			s.logger.Printf("[WARN] consul: failed to leave raft configuration gracefully, timeout")
		}
	}

	return nil
}

func (s *Server) numPeers() (int, error) {
	future := s.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return 0, err
	}

	return autopilot.NumPeers(future.Configuration()), nil
}

func (s *Server) JoinLAN(addrs []string) (int, error) {
	return s.serfLAN.Join(addrs, true)
}

func (s *Server) JoinWAN(addrs []string) (int, error) {
	if s.serfWAN == nil {
		return 0, ErrWANFederationDisabled
	}
	return s.serfWAN.Join(addrs, true)
}

func (s *Server) LocalMember() serf.Member {
	return s.serfLAN.LocalMember()
}

func (s *Server) LANMembers() []serf.Member {
	return s.serfLAN.Members()
}

func (s *Server) WANMembers() []serf.Member {
	if s.serfWAN == nil {
		return nil
	}
	return s.serfWAN.Members()
}

func (s *Server) RemoveFailedNode(node string) error {
	if err := s.serfLAN.RemoveFailedNode(node); err != nil {
		return err
	}
	if s.serfWAN != nil {
		if err := s.serfWAN.RemoveFailedNode(node); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

func (s *Server) KeyManagerLAN() *serf.KeyManager {
	return s.serfLAN.KeyManager()
}

func (s *Server) KeyManagerWAN() *serf.KeyManager {
	return s.serfWAN.KeyManager()
}

func (s *Server) Encrypted() bool {
	LANEncrypted := s.serfLAN.EncryptionEnabled()
	if s.serfWAN == nil {
		return LANEncrypted
	}
	return LANEncrypted && s.serfWAN.EncryptionEnabled()
}

func (s *Server) LANSegments() map[string]*serf.Serf {
	segments := make(map[string]*serf.Serf, len(s.segmentLAN)+1)
	segments[""] = s.serfLAN
	for name, segment := range s.segmentLAN {
		segments[name] = segment
	}

	return segments
}

type inmemCodec struct {
	method string
	args   interface{}
	reply  interface{}
	err    error
}

func (i *inmemCodec) ReadRequestHeader(req *rpc.Request) error {
	req.ServiceMethod = i.method
	return nil
}

func (i *inmemCodec) ReadRequestBody(args interface{}) error {
	sourceValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(i.args)))
	dst := reflect.Indirect(reflect.Indirect(reflect.ValueOf(args)))
	dst.Set(sourceValue)
	return nil
}

func (i *inmemCodec) WriteResponse(resp *rpc.Response, reply interface{}) error {
	if resp.Error != "" {
		i.err = errors.New(resp.Error)
		return nil
	}
	sourceValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(reply)))
	dst := reflect.Indirect(reflect.Indirect(reflect.ValueOf(i.reply)))
	dst.Set(sourceValue)
	return nil
}

func (i *inmemCodec) Close() error {
	return nil
}

func (s *Server) RPC(method string, args interface{}, reply interface{}) error {
	codec := &inmemCodec{
		method: method,
		args:   args,
		reply:  reply,
	}
	if err := s.rpcServer.ServeRequest(codec); err != nil {
		return err
	}
	return codec.err
}

func (s *Server) SnapshotRPC(args *structs.SnapshotRequest, in io.Reader, out io.Writer,
	replyFn structs.SnapshotReplyFn) error {

	var reply structs.SnapshotResponse
	snap, err := s.dispatchSnapshotRequest(args, in, &reply)
	if err != nil {
		return err
	}
	defer func() {
		if err := snap.Close(); err != nil {
			s.logger.Printf("[ERR] consul: Failed to close snapshot: %v", err)
		}
	}()

	if replyFn != nil {
		if err := replyFn(&reply); err != nil {
			return nil
		}
	}

	if out != nil {
		if _, err := io.Copy(out, snap); err != nil {
			return fmt.Errorf("failed to stream snapshot: %v", err)
		}
	}
	return nil
}

func (s *Server) RegisterEndpoint(name string, handler interface{}) error {
	s.logger.Printf("[WARN] consul: endpoint injected; this should only be used for testing")
	return s.rpcServer.RegisterName(name, handler)
}

func (s *Server) Stats() map[string]map[string]string {
	toString := func(v uint64) string {
		return strconv.FormatUint(v, 10)
	}
	numKnownDCs := len(s.router.GetDatacenters())
	stats := map[string]map[string]string{
		"consul": {
			"server":            "true",
			"leader":            fmt.Sprintf("%v", s.IsLeader()),
			"leader_addr":       string(s.raft.Leader()),
			"bootstrap":         fmt.Sprintf("%v", s.config.Bootstrap),
			"known_datacenters": toString(uint64(numKnownDCs)),
		},
		"raft":     s.raft.Stats(),
		"serf_lan": s.serfLAN.Stats(),
		"runtime":  runtimeStats(),
	}
	if s.serfWAN != nil {
		stats["serf_wan"] = s.serfWAN.Stats()
	}
	return stats
}

func (s *Server) GetLANCoordinate() (lib.CoordinateSet, error) {
	lan, err := s.serfLAN.GetCoordinate()
	if err != nil {
		return nil, err
	}

	cs := lib.CoordinateSet{"": lan}
	for name, segment := range s.segmentLAN {
		c, err := segment.GetCoordinate()
		if err != nil {
			return nil, err
		}
		cs[name] = c
	}
	return cs, nil
}

func (s *Server) setConsistentReadReady() {
	atomic.StoreInt32(&s.readyForConsistentReads, 1)
}

func (s *Server) resetConsistentReadReady() {
	atomic.StoreInt32(&s.readyForConsistentReads, 0)
}

func (s *Server) isReadyForConsistentReads() bool {
	return atomic.LoadInt32(&s.readyForConsistentReads) == 1
}

const peersInfoContent = `
As of Consul 0.7.0, the peers.json file is only used for recovery
after an outage. The format of this file depends on what the server has
configured for its Raft protocol version. Please see the agent configuration
page at https://www.consul.io/docs/agent/options.html#_raft_protocol for more
details about this parameter.

For Raft protocol version 2 and earlier, this should be formatted as a JSON
array containing the address and port of each Consul server in the cluster, like
this:

[
  "10.1.0.1:8300",
  "10.1.0.2:8300",
  "10.1.0.3:8300"
]

For Raft protocol version 3 and later, this should be formatted as a JSON
array containing the node ID, address:port, and suffrage information of each
Consul server in the cluster, like this:

[
  {
    "id": "adf4238a-882b-9ddc-4a9d-5b6758e4159e",
    "address": "10.1.0.1:8300",
    "non_voter": false
  },
  {
    "id": "8b6dda82-3103-11e7-93ae-92361f002671",
    "address": "10.1.0.2:8300",
    "non_voter": false
  },
  {
    "id": "97e17742-3103-11e7-93ae-92361f002671",
    "address": "10.1.0.3:8300",
    "non_voter": false
  }
]

The "id" field is the node ID of the server. This can be found in the logs when
the server starts up, or in the "node-id" file inside the server's data
directory.

The "address" field is the address and port of the server.

The "non_voter" field controls whether the server is a non-voter, which is used
in some advanced Autopilot configurations, please see
https://www.consul.io/docs/guides/autopilot.html for more information. If
"non_voter" is omitted it will default to false, which is typical for most
clusters.

Under normal operation, the peers.json file will not be present.

When Consul starts for the first time, it will create this peers.info file and
delete any existing peers.json file so that recovery doesn't occur on the first
startup.

Once this peers.info file is present, any peers.json file will be ingested at
startup, and will set the Raft peer configuration manually to recover from an
outage. It's crucial that all servers in the cluster are shut down before
creating the peers.json file, and that all servers receive the same
configuration. Once the peers.json file is successfully ingested and applied, it
will be deleted.

Please see https://www.consul.io/docs/guides/outage.html for more information.
`
