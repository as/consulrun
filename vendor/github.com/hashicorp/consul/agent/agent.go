package agent

import (
	"context"
	"crypto/sha512"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/ae"
	"github.com/hashicorp/consul/agent/checks"
	"github.com/hashicorp/consul/agent/config"
	"github.com/hashicorp/consul/agent/consul"
	"github.com/hashicorp/consul/agent/local"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/agent/systemd"
	"github.com/hashicorp/consul/agent/token"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/ipaddr"
	"github.com/hashicorp/consul/lib"
	"github.com/hashicorp/consul/logger"
	"github.com/hashicorp/consul/types"
	"github.com/hashicorp/consul/watch"
	"github.com/hashicorp/go-uuid"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/shirou/gopsutil/host"
	"golang.org/x/net/http2"
)

const (
	servicesDir = "services"

	checksDir     = "checks"
	checkStateDir = "checks/state"

	defaultNodeMaintReason = "Maintenance mode is enabled for this node, " +
		"but no reason was provided. This is a default message."
	defaultServiceMaintReason = "Maintenance mode is enabled for this " +
		"service, but no reason was provided. This is a default message."
)

type delegate interface {
	Encrypted() bool
	GetLANCoordinate() (lib.CoordinateSet, error)
	Leave() error
	LANMembers() []serf.Member
	LANMembersAllSegments() ([]serf.Member, error)
	LANSegmentMembers(segment string) ([]serf.Member, error)
	LocalMember() serf.Member
	JoinLAN(addrs []string) (n int, err error)
	RemoveFailedNode(node string) error
	RPC(method string, args interface{}, reply interface{}) error
	SnapshotRPC(args *structs.SnapshotRequest, in io.Reader, out io.Writer, replyFn structs.SnapshotReplyFn) error
	Shutdown() error
	Stats() map[string]map[string]string
}

type notifier interface {
	Notify(string) error
}

type Agent struct {
	config *config.RuntimeConfig

	logger *log.Logger

	LogOutput io.Writer

	LogWriter *logger.LogWriter

	MemSink *metrics.InmemSink

	delegate delegate

	acls *aclManager

	State *local.State

	sync *ae.StateSyncer

	checkReapAfter map[types.CheckID]time.Duration

	checkMonitors map[types.CheckID]*checks.CheckMonitor

	checkHTTPs map[types.CheckID]*checks.CheckHTTP

	checkTCPs map[types.CheckID]*checks.CheckTCP

	checkGRPCs map[types.CheckID]*checks.CheckGRPC

	checkTTLs map[types.CheckID]*checks.CheckTTL

	checkDockers map[types.CheckID]*checks.CheckDocker

	checkLock sync.Mutex

	dockerClient *checks.DockerClient

	eventCh chan serf.UserEvent

	eventBuf    []*UserEvent
	eventIndex  int
	eventLock   sync.RWMutex
	eventNotify NotifyGroup

	reloadCh chan chan error

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	joinLANNotifier notifier

	retryJoinCh chan error

	endpoints     map[string]string
	endpointsLock sync.RWMutex

	dnsServers []*DNSServer

	httpServers []*HTTPServer

	wgServers sync.WaitGroup

	watchPlans []*watch.Plan

	tokens *token.Store
}

func New(c *config.RuntimeConfig) (*Agent, error) {
	if c.Datacenter == "" {
		return nil, fmt.Errorf("Must configure a Datacenter")
	}
	if c.DataDir == "" && !c.DevMode {
		return nil, fmt.Errorf("Must configure a DataDir")
	}
	acls, err := newACLManager(c)
	if err != nil {
		return nil, err
	}

	a := &Agent{
		config:          c,
		acls:            acls,
		checkReapAfter:  make(map[types.CheckID]time.Duration),
		checkMonitors:   make(map[types.CheckID]*checks.CheckMonitor),
		checkTTLs:       make(map[types.CheckID]*checks.CheckTTL),
		checkHTTPs:      make(map[types.CheckID]*checks.CheckHTTP),
		checkTCPs:       make(map[types.CheckID]*checks.CheckTCP),
		checkGRPCs:      make(map[types.CheckID]*checks.CheckGRPC),
		checkDockers:    make(map[types.CheckID]*checks.CheckDocker),
		eventCh:         make(chan serf.UserEvent, 1024),
		eventBuf:        make([]*UserEvent, 256),
		joinLANNotifier: &systemd.Notifier{},
		reloadCh:        make(chan chan error),
		retryJoinCh:     make(chan error),
		shutdownCh:      make(chan struct{}),
		endpoints:       make(map[string]string),
		tokens:          new(token.Store),
	}

	a.tokens.UpdateUserToken(a.config.ACLToken)
	a.tokens.UpdateAgentToken(a.config.ACLAgentToken)
	a.tokens.UpdateAgentMasterToken(a.config.ACLAgentMasterToken)
	a.tokens.UpdateACLReplicationToken(a.config.ACLReplicationToken)

	return a, nil
}

func LocalConfig(cfg *config.RuntimeConfig) local.Config {
	lc := local.Config{
		AdvertiseAddr:       cfg.AdvertiseAddrLAN.String(),
		CheckUpdateInterval: cfg.CheckUpdateInterval,
		Datacenter:          cfg.Datacenter,
		DiscardCheckOutput:  cfg.DiscardCheckOutput,
		NodeID:              cfg.NodeID,
		NodeName:            cfg.NodeName,
		TaggedAddresses:     map[string]string{},
	}
	for k, v := range cfg.TaggedAddresses {
		lc.TaggedAddresses[k] = v
	}
	return lc
}

func (a *Agent) Start() error {
	c := a.config

	logOutput := a.LogOutput
	if a.logger == nil {
		if logOutput == nil {
			logOutput = os.Stderr
		}
		a.logger = log.New(logOutput, "", log.LstdFlags)
	}

	if err := a.setupNodeID(c); err != nil {
		return fmt.Errorf("Failed to setup node ID: %v", err)
	}

	if InvalidDnsRe.MatchString(a.config.NodeName) {
		a.logger.Printf("[WARN] agent: Node name %q will not be discoverable "+
			"via DNS due to invalid characters. Valid characters include "+
			"all alpha-numerics and dashes.", a.config.NodeName)
	} else if len(a.config.NodeName) > MaxDNSLabelLength {
		a.logger.Printf("[WARN] agent: Node name %q will not be discoverable "+
			"via DNS due to it being too long. Valid lengths are between "+
			"1 and 63 bytes.", a.config.NodeName)
	}

	a.State = local.NewState(LocalConfig(c), a.logger, a.tokens)

	a.sync = ae.NewStateSyncer(a.State, c.AEInterval, a.shutdownCh, a.logger)

	consulCfg, err := a.consulConfig()
	if err != nil {
		return err
	}

	consulCfg.ServerUp = a.sync.SyncFull.Trigger

	if c.ServerMode {
		server, err := consul.NewServerLogger(consulCfg, a.logger, a.tokens)
		if err != nil {
			return fmt.Errorf("Failed to start Consul server: %v", err)
		}
		a.delegate = server
	} else {
		client, err := consul.NewClientLogger(consulCfg, a.logger)
		if err != nil {
			return fmt.Errorf("Failed to start Consul client: %v", err)
		}
		a.delegate = client
	}

	a.sync.ClusterSize = func() int { return len(a.delegate.LANMembers()) }

	a.State.Delegate = a.delegate
	a.State.TriggerSyncChanges = a.sync.SyncChanges.Trigger

	if err := a.loadServices(c); err != nil {
		return err
	}
	if err := a.loadChecks(c); err != nil {
		return err
	}
	if err := a.loadMetadata(c); err != nil {
		return err
	}

	go a.reapServices()

	go a.handleEvents()

	if !c.DisableCoordinates {
		go a.sendCoordinate()
	}

	if err := a.storePid(); err != nil {
		return err
	}

	if err := a.listenAndServeDNS(); err != nil {
		return err
	}

	servers, err := a.listenHTTP()
	if err != nil {
		return err
	}

	for _, srv := range servers {
		if err := a.serveHTTP(srv); err != nil {
			return err
		}
		a.httpServers = append(a.httpServers, srv)
	}

	if err := a.reloadWatches(a.config); err != nil {
		return err
	}

	go a.retryJoinLAN()
	go a.retryJoinWAN()

	return nil
}

func (a *Agent) listenAndServeDNS() error {
	notif := make(chan net.Addr, len(a.config.DNSAddrs))
	for _, addr := range a.config.DNSAddrs {

		s, err := NewDNSServer(a)
		if err != nil {
			return err
		}
		a.dnsServers = append(a.dnsServers, s)

		a.wgServers.Add(1)
		go func(addr net.Addr) {
			defer a.wgServers.Done()
			err := s.ListenAndServe(addr.Network(), addr.String(), func() { notif <- addr })
			if err != nil && !strings.Contains(err.Error(), "accept") {
				a.logger.Printf("[ERR] agent: Error starting DNS server %s (%s): %v", addr.String(), addr.Network(), err)
			}
		}(addr)
	}

	timeout := time.After(time.Second)
	for range a.config.DNSAddrs {
		select {
		case addr := <-notif:
			a.logger.Printf("[INFO] agent: Started DNS server %s (%s)", addr.String(), addr.Network())
			continue
		case <-timeout:
			return fmt.Errorf("agent: timeout starting DNS servers")
		}
	}
	return nil
}

//
//
func (a *Agent) listenHTTP() ([]*HTTPServer, error) {
	var ln []net.Listener
	var servers []*HTTPServer
	start := func(proto string, addrs []net.Addr) error {
		for _, addr := range addrs {
			var l net.Listener
			var tlscfg *tls.Config
			var err error

			switch x := addr.(type) {
			case *net.UnixAddr:
				l, err = a.listenSocket(x.Name)
				if err != nil {
					return err
				}

			case *net.TCPAddr:
				l, err = net.Listen("tcp", x.String())
				if err != nil {
					return err
				}
				l = &tcpKeepAliveListener{l.(*net.TCPListener)}

				if proto == "https" {
					tlscfg, err = a.config.IncomingHTTPSConfig()
					if err != nil {
						return err
					}
					l = tls.NewListener(l, tlscfg)
				}

			default:
				return fmt.Errorf("unsupported address type %T", addr)
			}
			ln = append(ln, l)

			srv := &HTTPServer{
				Server: &http.Server{
					Addr:      l.Addr().String(),
					TLSConfig: tlscfg,
				},
				ln:        l,
				agent:     a,
				blacklist: NewBlacklist(a.config.HTTPBlockEndpoints),
				proto:     proto,
			}
			srv.Server.Handler = srv.handler(a.config.EnableDebug)

			if proto == "https" {
				err = http2.ConfigureServer(srv.Server, nil)
				if err != nil {
					return err
				}
			}

			servers = append(servers, srv)
		}
		return nil
	}

	if err := start("http", a.config.HTTPAddrs); err != nil {
		for _, l := range ln {
			l.Close()
		}
		return nil, err
	}
	if err := start("https", a.config.HTTPSAddrs); err != nil {
		for _, l := range ln {
			l.Close()
		}
		return nil, err
	}
	return servers, nil
}

type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(30 * time.Second)
	return tc, nil
}

func (a *Agent) listenSocket(path string) (net.Listener, error) {
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		a.logger.Printf("[WARN] agent: Replacing socket %q", path)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("error removing socket file: %s", err)
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	user, group, mode := a.config.UnixSocketUser, a.config.UnixSocketGroup, a.config.UnixSocketMode
	if err := setFilePermissions(path, user, group, mode); err != nil {
		return nil, fmt.Errorf("Failed setting up socket: %s", err)
	}
	return l, nil
}

func (a *Agent) serveHTTP(srv *HTTPServer) error {

	//

	notif := make(chan net.Addr)
	a.wgServers.Add(1)
	go func() {
		defer a.wgServers.Done()
		notif <- srv.ln.Addr()
		err := srv.Serve(srv.ln)
		if err != nil && err != http.ErrServerClosed {
			a.logger.Print(err)
		}
	}()

	select {
	case addr := <-notif:
		if srv.proto == "https" {
			a.logger.Printf("[INFO] agent: Started HTTPS server on %s (%s)", addr.String(), addr.Network())
		} else {
			a.logger.Printf("[INFO] agent: Started HTTP server on %s (%s)", addr.String(), addr.Network())
		}
		return nil
	case <-time.After(time.Second):
		return fmt.Errorf("agent: timeout starting HTTP servers")
	}
}

func (a *Agent) reloadWatches(cfg *config.RuntimeConfig) error {

	for _, wp := range a.watchPlans {
		wp.Stop()
	}
	a.watchPlans = nil

	if len(cfg.Watches) == 0 {
		return nil
	}

	if len(cfg.HTTPAddrs) == 0 && len(cfg.HTTPSAddrs) == 0 {
		return fmt.Errorf("watch plans require an HTTP or HTTPS endpoint")
	}

	var watchPlans []*watch.Plan
	for _, params := range cfg.Watches {
		if handlerType, ok := params["handler_type"]; !ok {
			params["handler_type"] = "script"
		} else if handlerType != "http" && handlerType != "script" {
			return fmt.Errorf("Handler type '%s' not recognized", params["handler_type"])
		}

		wp, err := watch.ParseExempt(params, []string{"handler", "args"})
		if err != nil {
			return fmt.Errorf("Failed to parse watch (%#v): %v", params, err)
		}

		handler, hasHandler := wp.Exempt["handler"]
		args, hasArgs := wp.Exempt["args"]
		if hasHandler {
			a.logger.Printf("[WARN] agent: The 'handler' field in watches has been deprecated " +
				"and replaced with the 'args' field. See https://www.consul.io/docs/agent/watches.html")
		}
		if _, ok := handler.(string); hasHandler && !ok {
			return fmt.Errorf("Watch handler must be a string")
		}
		if raw, ok := args.([]interface{}); hasArgs && ok {
			var parsed []string
			for _, arg := range raw {
				v, ok := arg.(string)
				if !ok {
					return fmt.Errorf("Watch args must be a list of strings")
				}

				parsed = append(parsed, v)
			}
			wp.Exempt["args"] = parsed
		} else if hasArgs && !ok {
			return fmt.Errorf("Watch args must be a list of strings")
		}
		if hasHandler && hasArgs || hasHandler && wp.HandlerType == "http" || hasArgs && wp.HandlerType == "http" {
			return fmt.Errorf("Only one watch handler allowed")
		}
		if !hasHandler && !hasArgs && wp.HandlerType != "http" {
			return fmt.Errorf("Must define a watch handler")
		}

		watchPlans = append(watchPlans, wp)
	}

	var netaddr net.Addr
	if len(cfg.HTTPAddrs) > 0 {
		netaddr = cfg.HTTPAddrs[0]
	} else {
		netaddr = cfg.HTTPSAddrs[0]
	}
	addr := netaddr.String()
	if netaddr.Network() == "unix" {
		addr = "unix://" + addr
	}

	for _, wp := range watchPlans {
		a.watchPlans = append(a.watchPlans, wp)
		go func(wp *watch.Plan) {
			if h, ok := wp.Exempt["handler"]; ok {
				wp.Handler = makeWatchHandler(a.LogOutput, h)
			} else if h, ok := wp.Exempt["args"]; ok {
				wp.Handler = makeWatchHandler(a.LogOutput, h)
			} else {
				httpConfig := wp.Exempt["http_handler_config"].(*watch.HttpHandlerConfig)
				wp.Handler = makeHTTPWatchHandler(a.LogOutput, httpConfig)
			}
			wp.LogOutput = a.LogOutput
			if err := wp.Run(addr); err != nil {
				a.logger.Printf("[ERR] agent: Failed to run watch: %v", err)
			}
		}(wp)
	}
	return nil
}

func (a *Agent) consulConfig() (*consul.Config, error) {

	base := consul.DefaultConfig()

	base.NodeID = a.config.NodeID

	base.DevMode = a.config.DevMode

	base.Datacenter = a.config.Datacenter
	base.DataDir = a.config.DataDir
	base.NodeName = a.config.NodeName

	base.CoordinateUpdateBatchSize = a.config.ConsulCoordinateUpdateBatchSize
	base.CoordinateUpdateMaxBatches = a.config.ConsulCoordinateUpdateMaxBatches
	base.CoordinateUpdatePeriod = a.config.ConsulCoordinateUpdatePeriod

	base.RaftConfig.HeartbeatTimeout = a.config.ConsulRaftHeartbeatTimeout
	base.RaftConfig.LeaderLeaseTimeout = a.config.ConsulRaftLeaderLeaseTimeout
	base.RaftConfig.ElectionTimeout = a.config.ConsulRaftElectionTimeout

	base.SerfLANConfig.MemberlistConfig.BindAddr = a.config.SerfBindAddrLAN.IP.String()
	base.SerfLANConfig.MemberlistConfig.BindPort = a.config.SerfBindAddrLAN.Port
	base.SerfLANConfig.MemberlistConfig.AdvertiseAddr = a.config.SerfAdvertiseAddrLAN.IP.String()
	base.SerfLANConfig.MemberlistConfig.AdvertisePort = a.config.SerfAdvertiseAddrLAN.Port
	base.SerfLANConfig.MemberlistConfig.GossipVerifyIncoming = a.config.EncryptVerifyIncoming
	base.SerfLANConfig.MemberlistConfig.GossipVerifyOutgoing = a.config.EncryptVerifyOutgoing
	base.SerfLANConfig.MemberlistConfig.GossipInterval = a.config.ConsulSerfLANGossipInterval
	base.SerfLANConfig.MemberlistConfig.ProbeInterval = a.config.ConsulSerfLANProbeInterval
	base.SerfLANConfig.MemberlistConfig.ProbeTimeout = a.config.ConsulSerfLANProbeTimeout
	base.SerfLANConfig.MemberlistConfig.SuspicionMult = a.config.ConsulSerfLANSuspicionMult

	if a.config.SerfBindAddrWAN != nil {
		base.SerfWANConfig.MemberlistConfig.BindAddr = a.config.SerfBindAddrWAN.IP.String()
		base.SerfWANConfig.MemberlistConfig.BindPort = a.config.SerfBindAddrWAN.Port
		base.SerfWANConfig.MemberlistConfig.AdvertiseAddr = a.config.SerfAdvertiseAddrWAN.IP.String()
		base.SerfWANConfig.MemberlistConfig.AdvertisePort = a.config.SerfAdvertiseAddrWAN.Port
		base.SerfWANConfig.MemberlistConfig.GossipVerifyIncoming = a.config.EncryptVerifyIncoming
		base.SerfWANConfig.MemberlistConfig.GossipVerifyOutgoing = a.config.EncryptVerifyOutgoing
		base.SerfWANConfig.MemberlistConfig.GossipInterval = a.config.ConsulSerfWANGossipInterval
		base.SerfWANConfig.MemberlistConfig.ProbeInterval = a.config.ConsulSerfWANProbeInterval
		base.SerfWANConfig.MemberlistConfig.ProbeTimeout = a.config.ConsulSerfWANProbeTimeout
		base.SerfWANConfig.MemberlistConfig.SuspicionMult = a.config.ConsulSerfWANSuspicionMult
	} else {

		base.SerfWANConfig = nil
	}

	base.RPCAddr = a.config.RPCBindAddr
	base.RPCAdvertise = a.config.RPCAdvertiseAddr

	if a.config.ReconnectTimeoutLAN != 0 {
		base.SerfLANConfig.ReconnectTimeout = a.config.ReconnectTimeoutLAN
	}
	if a.config.ReconnectTimeoutWAN != 0 {
		base.SerfWANConfig.ReconnectTimeout = a.config.ReconnectTimeoutWAN
	}

	base.Segment = a.config.SegmentName
	if len(a.config.Segments) > 0 {
		segments, err := a.segmentConfig()
		if err != nil {
			return nil, err
		}
		base.Segments = segments
	}
	if a.config.Bootstrap {
		base.Bootstrap = true
	}
	if a.config.RejoinAfterLeave {
		base.RejoinAfterLeave = true
	}
	if a.config.BootstrapExpect != 0 {
		base.BootstrapExpect = a.config.BootstrapExpect
	}
	if a.config.RPCProtocol > 0 {
		base.ProtocolVersion = uint8(a.config.RPCProtocol)
	}
	if a.config.RaftProtocol != 0 {
		base.RaftConfig.ProtocolVersion = raft.ProtocolVersion(a.config.RaftProtocol)
	}
	if a.config.ACLMasterToken != "" {
		base.ACLMasterToken = a.config.ACLMasterToken
	}
	if a.config.ACLDatacenter != "" {
		base.ACLDatacenter = a.config.ACLDatacenter
	}
	if a.config.ACLTTL != 0 {
		base.ACLTTL = a.config.ACLTTL
	}
	if a.config.ACLDefaultPolicy != "" {
		base.ACLDefaultPolicy = a.config.ACLDefaultPolicy
	}
	if a.config.ACLDownPolicy != "" {
		base.ACLDownPolicy = a.config.ACLDownPolicy
	}
	base.EnableACLReplication = a.config.EnableACLReplication
	if a.config.ACLEnforceVersion8 {
		base.ACLEnforceVersion8 = a.config.ACLEnforceVersion8
	}
	if a.config.ACLEnableKeyListPolicy {
		base.ACLEnableKeyListPolicy = a.config.ACLEnableKeyListPolicy
	}
	if a.config.SessionTTLMin != 0 {
		base.SessionTTLMin = a.config.SessionTTLMin
	}
	if a.config.NonVotingServer {
		base.NonVoter = a.config.NonVotingServer
	}

	base.AutopilotConfig.CleanupDeadServers = a.config.AutopilotCleanupDeadServers
	base.AutopilotConfig.LastContactThreshold = a.config.AutopilotLastContactThreshold
	base.AutopilotConfig.MaxTrailingLogs = uint64(a.config.AutopilotMaxTrailingLogs)
	base.AutopilotConfig.ServerStabilizationTime = a.config.AutopilotServerStabilizationTime
	base.AutopilotConfig.RedundancyZoneTag = a.config.AutopilotRedundancyZoneTag
	base.AutopilotConfig.DisableUpgradeMigration = a.config.AutopilotDisableUpgradeMigration
	base.AutopilotConfig.UpgradeVersionTag = a.config.AutopilotUpgradeVersionTag

	if base.RPCAdvertise == nil {
		base.RPCAdvertise = base.RPCAddr
	}

	if a.config.RPCRateLimit > 0 {
		base.RPCRate = a.config.RPCRateLimit
	}
	if a.config.RPCMaxBurst > 0 {
		base.RPCMaxBurst = a.config.RPCMaxBurst
	}

	if a.config.RPCHoldTimeout > 0 {
		base.RPCHoldTimeout = a.config.RPCHoldTimeout
	}
	if a.config.LeaveDrainTime > 0 {
		base.LeaveDrainTime = a.config.LeaveDrainTime
	}

	if !ipaddr.IsAny(base.RPCAddr.IP) {
		base.RPCSrcAddr = &net.TCPAddr{IP: base.RPCAddr.IP}
	}

	revision := a.config.Revision
	if len(revision) > 8 {
		revision = revision[:8]
	}
	base.Build = fmt.Sprintf("%s%s:%s", a.config.Version, a.config.VersionPrerelease, revision)

	base.VerifyIncoming = a.config.VerifyIncoming || a.config.VerifyIncomingRPC
	if a.config.CAPath != "" || a.config.CAFile != "" {
		base.UseTLS = true
	}
	base.VerifyOutgoing = a.config.VerifyOutgoing
	base.VerifyServerHostname = a.config.VerifyServerHostname
	base.CAFile = a.config.CAFile
	base.CAPath = a.config.CAPath
	base.CertFile = a.config.CertFile
	base.KeyFile = a.config.KeyFile
	base.ServerName = a.config.ServerName
	base.Domain = a.config.DNSDomain
	base.TLSMinVersion = a.config.TLSMinVersion
	base.TLSCipherSuites = a.config.TLSCipherSuites
	base.TLSPreferServerCipherSuites = a.config.TLSPreferServerCipherSuites

	base.UserEventHandler = func(e serf.UserEvent) {
		select {
		case a.eventCh <- e:
		case <-a.shutdownCh:
		}
	}

	base.LogOutput = a.LogOutput

	if err := a.setupKeyrings(base); err != nil {
		return nil, fmt.Errorf("Failed to configure keyring: %v", err)
	}

	return base, nil
}

func (a *Agent) segmentConfig() ([]consul.NetworkSegment, error) {
	var segments []consul.NetworkSegment
	config := a.config

	for _, s := range config.Segments {
		serfConf := consul.DefaultConfig().SerfLANConfig

		serfConf.MemberlistConfig.BindAddr = s.Bind.IP.String()
		serfConf.MemberlistConfig.BindPort = s.Bind.Port
		serfConf.MemberlistConfig.AdvertiseAddr = s.Advertise.IP.String()
		serfConf.MemberlistConfig.AdvertisePort = s.Advertise.Port

		if config.ReconnectTimeoutLAN != 0 {
			serfConf.ReconnectTimeout = config.ReconnectTimeoutLAN
		}
		if config.EncryptVerifyIncoming {
			serfConf.MemberlistConfig.GossipVerifyIncoming = config.EncryptVerifyIncoming
		}
		if config.EncryptVerifyOutgoing {
			serfConf.MemberlistConfig.GossipVerifyOutgoing = config.EncryptVerifyOutgoing
		}

		var rpcAddr *net.TCPAddr
		if s.RPCListener {
			rpcAddr = &net.TCPAddr{
				IP:   s.Bind.IP,
				Port: a.config.ServerPort,
			}
		}

		segments = append(segments, consul.NetworkSegment{
			Name:       s.Name,
			Bind:       serfConf.MemberlistConfig.BindAddr,
			Advertise:  serfConf.MemberlistConfig.AdvertiseAddr,
			Port:       s.Bind.Port,
			RPCAddr:    rpcAddr,
			SerfConfig: serfConf,
		})
	}

	return segments, nil
}

func (a *Agent) makeRandomID() (string, error) {
	id, err := uuid.GenerateUUID()
	if err != nil {
		return "", err
	}

	a.logger.Printf("[DEBUG] agent: Using random ID %q as node ID", id)
	return id, nil
}

func (a *Agent) makeNodeID() (string, error) {

	if a.config.DisableHostNodeID {
		return a.makeRandomID()
	}

	info, err := host.Info()
	if err != nil {
		a.logger.Printf("[DEBUG] agent: Couldn't get a unique ID from the host: %v", err)
		return a.makeRandomID()
	}

	id := strings.ToLower(info.HostID)
	if _, err := uuid.ParseUUID(id); err != nil {
		a.logger.Printf("[DEBUG] agent: Unique ID %q from host isn't formatted as a UUID: %v",
			id, err)
		return a.makeRandomID()
	}

	buf := sha512.Sum512([]byte(id))
	id = fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])

	a.logger.Printf("[DEBUG] agent: Using unique ID %q from host as node ID", id)
	return id, nil
}

func (a *Agent) setupNodeID(config *config.RuntimeConfig) error {

	if config.NodeID != "" {
		config.NodeID = types.NodeID(strings.ToLower(string(config.NodeID)))
		if _, err := uuid.ParseUUID(string(config.NodeID)); err != nil {
			return err
		}

		return nil
	}

	if a.config.DevMode {
		id, err := a.makeNodeID()
		if err != nil {
			return err
		}

		config.NodeID = types.NodeID(id)
		return nil
	}

	fileID := filepath.Join(config.DataDir, "node-id")
	if _, err := os.Stat(fileID); err == nil {
		rawID, err := ioutil.ReadFile(fileID)
		if err != nil {
			return err
		}

		nodeID := strings.TrimSpace(string(rawID))
		nodeID = strings.ToLower(nodeID)
		if _, err := uuid.ParseUUID(nodeID); err != nil {
			return err
		}

		config.NodeID = types.NodeID(nodeID)
	}

	if config.NodeID == "" {
		id, err := a.makeNodeID()
		if err != nil {
			return err
		}
		if err := lib.EnsurePath(fileID, false); err != nil {
			return err
		}
		if err := ioutil.WriteFile(fileID, []byte(id), 0600); err != nil {
			return err
		}

		config.NodeID = types.NodeID(id)
	}
	return nil
}

func (a *Agent) setupBaseKeyrings(config *consul.Config) error {

	federationEnabled := config.SerfWANConfig != nil
	if a.config.DisableKeyringFile {
		if a.config.EncryptKey == "" {
			return nil
		}

		keys := []string{a.config.EncryptKey}
		if err := loadKeyring(config.SerfLANConfig, keys); err != nil {
			return err
		}
		if a.config.ServerMode && federationEnabled {
			if err := loadKeyring(config.SerfWANConfig, keys); err != nil {
				return err
			}
		}
		return nil
	}

	fileLAN := filepath.Join(a.config.DataDir, SerfLANKeyring)
	fileWAN := filepath.Join(a.config.DataDir, SerfWANKeyring)

	if a.config.EncryptKey == "" {
		goto LOAD
	}
	if _, err := os.Stat(fileLAN); err != nil {
		if err := initKeyring(fileLAN, a.config.EncryptKey); err != nil {
			return err
		}
	}
	if a.config.ServerMode && federationEnabled {
		if _, err := os.Stat(fileWAN); err != nil {
			if err := initKeyring(fileWAN, a.config.EncryptKey); err != nil {
				return err
			}
		}
	}

LOAD:
	if _, err := os.Stat(fileLAN); err == nil {
		config.SerfLANConfig.KeyringFile = fileLAN
	}
	if err := loadKeyringFile(config.SerfLANConfig); err != nil {
		return err
	}
	if a.config.ServerMode && federationEnabled {
		if _, err := os.Stat(fileWAN); err == nil {
			config.SerfWANConfig.KeyringFile = fileWAN
		}
		if err := loadKeyringFile(config.SerfWANConfig); err != nil {
			return err
		}
	}

	return nil
}

func (a *Agent) setupKeyrings(config *consul.Config) error {

	if err := a.setupBaseKeyrings(config); err != nil {
		return err
	}

	lanKeyring := config.SerfLANConfig.MemberlistConfig.Keyring
	if lanKeyring == nil {
		return nil
	}

	k, pk := lanKeyring.GetKeys(), lanKeyring.GetPrimaryKey()
	for _, segment := range config.Segments {
		keyring, err := memberlist.NewKeyring(k, pk)
		if err != nil {
			return err
		}
		segment.SerfConfig.MemberlistConfig.Keyring = keyring
	}
	return nil
}

func (a *Agent) registerEndpoint(name string, handler interface{}) error {
	srv, ok := a.delegate.(*consul.Server)
	if !ok {
		panic("agent must be a server")
	}
	realname := fmt.Sprintf("%s-%d", name, time.Now().UnixNano())
	a.endpointsLock.Lock()
	a.endpoints[name] = realname
	a.endpointsLock.Unlock()
	return srv.RegisterEndpoint(realname, handler)
}

func (a *Agent) RPC(method string, args interface{}, reply interface{}) error {
	a.endpointsLock.RLock()

	if len(a.endpoints) > 0 {
		p := strings.SplitN(method, ".", 2)
		if e := a.endpoints[p[0]]; e != "" {
			method = e + "." + p[1]
		}
	}
	a.endpointsLock.RUnlock()
	return a.delegate.RPC(method, args, reply)
}

func (a *Agent) SnapshotRPC(args *structs.SnapshotRequest, in io.Reader, out io.Writer,
	replyFn structs.SnapshotReplyFn) error {
	return a.delegate.SnapshotRPC(args, in, out, replyFn)
}

func (a *Agent) Leave() error {
	return a.delegate.Leave()
}

func (a *Agent) ShutdownAgent() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}
	a.logger.Println("[INFO] agent: Requesting shutdown")

	a.checkLock.Lock()
	defer a.checkLock.Unlock()
	for _, chk := range a.checkMonitors {
		chk.Stop()
	}
	for _, chk := range a.checkTTLs {
		chk.Stop()
	}
	for _, chk := range a.checkHTTPs {
		chk.Stop()
	}
	for _, chk := range a.checkTCPs {
		chk.Stop()
	}
	for _, chk := range a.checkGRPCs {
		chk.Stop()
	}
	for _, chk := range a.checkDockers {
		chk.Stop()
	}

	var err error
	if a.delegate != nil {
		err = a.delegate.Shutdown()
		if _, ok := a.delegate.(*consul.Server); ok {
			a.logger.Print("[INFO] agent: consul server down")
		} else {
			a.logger.Print("[INFO] agent: consul client down")
		}
	}

	pidErr := a.deletePid()
	if pidErr != nil {
		a.logger.Println("[WARN] agent: could not delete pid file ", pidErr)
	}

	a.logger.Println("[INFO] agent: shutdown complete")
	a.shutdown = true
	close(a.shutdownCh)
	return err
}

func (a *Agent) ShutdownEndpoints() {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if len(a.dnsServers) == 0 && len(a.httpServers) == 0 {
		return
	}

	for _, srv := range a.dnsServers {
		a.logger.Printf("[INFO] agent: Stopping DNS server %s (%s)", srv.Server.Addr, srv.Server.Net)
		srv.Shutdown()
	}
	a.dnsServers = nil

	for _, srv := range a.httpServers {
		a.logger.Printf("[INFO] agent: Stopping %s server %s (%s)", strings.ToUpper(srv.proto), srv.ln.Addr().String(), srv.ln.Addr().Network())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		if ctx.Err() == context.DeadlineExceeded {
			a.logger.Printf("[WARN] agent: Timeout stopping %s server %s (%s)", strings.ToUpper(srv.proto), srv.ln.Addr().String(), srv.ln.Addr().Network())
		}
	}
	a.httpServers = nil

	a.logger.Println("[INFO] agent: Waiting for endpoints to shut down")
	a.wgServers.Wait()
	a.logger.Print("[INFO] agent: Endpoints down")
}

func (a *Agent) ReloadCh() chan chan error {
	return a.reloadCh
}

func (a *Agent) RetryJoinCh() <-chan error {
	return a.retryJoinCh
}

func (a *Agent) ShutdownCh() <-chan struct{} {
	return a.shutdownCh
}

func (a *Agent) JoinLAN(addrs []string) (n int, err error) {
	a.logger.Printf("[INFO] agent: (LAN) joining: %v", addrs)
	n, err = a.delegate.JoinLAN(addrs)
	a.logger.Printf("[INFO] agent: (LAN) joined: %d Err: %v", n, err)
	if err == nil && a.joinLANNotifier != nil {
		if notifErr := a.joinLANNotifier.Notify(systemd.Ready); notifErr != nil {
			a.logger.Printf("[DEBUG] agent: systemd notify failed: %v", notifErr)
		}
	}
	return
}

func (a *Agent) JoinWAN(addrs []string) (n int, err error) {
	a.logger.Printf("[INFO] agent: (WAN) joining: %v", addrs)
	if srv, ok := a.delegate.(*consul.Server); ok {
		n, err = srv.JoinWAN(addrs)
	} else {
		err = fmt.Errorf("Must be a server to join WAN cluster")
	}
	a.logger.Printf("[INFO] agent: (WAN) joined: %d Err: %v", n, err)
	return
}

func (a *Agent) ForceLeave(node string) (err error) {
	a.logger.Printf("[INFO] agent: Force leaving node: %v", node)
	err = a.delegate.RemoveFailedNode(node)
	if err != nil {
		a.logger.Printf("[WARN] agent: Failed to remove node: %v", err)
	}
	return err
}

func (a *Agent) LocalMember() serf.Member {
	return a.delegate.LocalMember()
}

func (a *Agent) LANMembers() []serf.Member {
	return a.delegate.LANMembers()
}

func (a *Agent) WANMembers() []serf.Member {
	if srv, ok := a.delegate.(*consul.Server); ok {
		return srv.WANMembers()
	}
	return nil
}

func (a *Agent) StartSync() {
	go a.sync.Run()
	a.logger.Printf("[INFO] agent: started state syncer")
}

func (a *Agent) PauseSync() {
	a.sync.Pause()
}

func (a *Agent) ResumeSync() {
	a.sync.Resume()
}

func (a *Agent) GetLANCoordinate() (lib.CoordinateSet, error) {
	return a.delegate.GetLANCoordinate()
}

func (a *Agent) sendCoordinate() {
OUTER:
	for {
		rate := a.config.SyncCoordinateRateTarget
		min := a.config.SyncCoordinateIntervalMin
		intv := lib.RateScaledInterval(rate, min, len(a.LANMembers()))
		intv = intv + lib.RandomStagger(intv)

		select {
		case <-time.After(intv):
			members := a.LANMembers()
			grok, err := consul.CanServersUnderstandProtocol(members, 3)
			if err != nil {
				a.logger.Printf("[ERR] agent: Failed to check servers: %s", err)
				continue
			}
			if !grok {
				a.logger.Printf("[DEBUG] agent: Skipping coordinate updates until servers are upgraded")
				continue
			}

			cs, err := a.GetLANCoordinate()
			if err != nil {
				a.logger.Printf("[ERR] agent: Failed to get coordinate: %s", err)
				continue
			}

			for segment, coord := range cs {
				req := structs.CoordinateUpdateRequest{
					Datacenter:   a.config.Datacenter,
					Node:         a.config.NodeName,
					Segment:      segment,
					Coord:        coord,
					WriteRequest: structs.WriteRequest{Token: a.tokens.AgentToken()},
				}
				var reply struct{}
				if err := a.RPC("Coordinate.Update", &req, &reply); err != nil {
					if acl.IsErrPermissionDenied(err) {
						a.logger.Printf("[WARN] agent: Coordinate update blocked by ACLs")
					} else {
						a.logger.Printf("[ERR] agent: Coordinate update error: %v", err)
					}
					continue OUTER
				}
			}
		case <-a.shutdownCh:
			return
		}
	}
}

func (a *Agent) reapServicesInternal() {
	reaped := make(map[string]bool)
	for checkID, cs := range a.State.CriticalCheckStates() {
		serviceID := cs.Check.ServiceID

		if serviceID == "" {
			continue
		}

		if reaped[serviceID] {
			continue
		}

		a.checkLock.Lock()
		timeout := a.checkReapAfter[checkID]
		a.checkLock.Unlock()

		if timeout > 0 && cs.CriticalFor() > timeout {
			reaped[serviceID] = true
			a.RemoveService(serviceID, true)
			a.logger.Printf("[INFO] agent: Check %q for service %q has been critical for too long; deregistered service",
				checkID, serviceID)
		}
	}
}

func (a *Agent) reapServices() {
	for {
		select {
		case <-time.After(a.config.CheckReapInterval):
			a.reapServicesInternal()

		case <-a.shutdownCh:
			return
		}
	}

}

type persistedService struct {
	Token   string
	Service *structs.NodeService
}

func (a *Agent) persistService(service *structs.NodeService) error {
	svcPath := filepath.Join(a.config.DataDir, servicesDir, stringHash(service.ID))

	wrapped := persistedService{
		Token:   a.State.ServiceToken(service.ID),
		Service: service,
	}
	encoded, err := json.Marshal(wrapped)
	if err != nil {
		return err
	}

	return writeFileAtomic(svcPath, encoded)
}

func (a *Agent) purgeService(serviceID string) error {
	svcPath := filepath.Join(a.config.DataDir, servicesDir, stringHash(serviceID))
	if _, err := os.Stat(svcPath); err == nil {
		return os.Remove(svcPath)
	}
	return nil
}

func (a *Agent) persistCheck(check *structs.HealthCheck, chkType *structs.CheckType) error {
	checkPath := filepath.Join(a.config.DataDir, checksDir, checkIDHash(check.CheckID))

	wrapped := persistedCheck{
		Check:   check,
		ChkType: chkType,
		Token:   a.State.CheckToken(check.CheckID),
	}

	encoded, err := json.Marshal(wrapped)
	if err != nil {
		return err
	}

	return writeFileAtomic(checkPath, encoded)
}

func (a *Agent) purgeCheck(checkID types.CheckID) error {
	checkPath := filepath.Join(a.config.DataDir, checksDir, checkIDHash(checkID))
	if _, err := os.Stat(checkPath); err == nil {
		return os.Remove(checkPath)
	}
	return nil
}

func writeFileAtomic(path string, contents []byte) error {
	uuid, err := uuid.GenerateUUID()
	if err != nil {
		return err
	}
	tempPath := fmt.Sprintf("%s-%s.tmp", path, uuid)

	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	fh, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err := fh.Write(contents); err != nil {
		fh.Close()
		os.Remove(tempPath)
		return err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		os.Remove(tempPath)
		return err
	}
	if err := fh.Close(); err != nil {
		os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return err
	}
	return nil
}

func (a *Agent) AddService(service *structs.NodeService, chkTypes []*structs.CheckType, persist bool, token string) error {
	if service.Service == "" {
		return fmt.Errorf("Service name missing")
	}
	if service.ID == "" && service.Service != "" {
		service.ID = service.Service
	}
	for _, check := range chkTypes {
		if err := check.Validate(); err != nil {
			return fmt.Errorf("Check is not valid: %v", err)
		}
	}

	if InvalidDnsRe.MatchString(service.Service) {
		a.logger.Printf("[WARN] agent: Service name %q will not be discoverable "+
			"via DNS due to invalid characters. Valid characters include "+
			"all alpha-numerics and dashes.", service.Service)
	} else if len(service.Service) > MaxDNSLabelLength {
		a.logger.Printf("[WARN] agent: Service name %q will not be discoverable "+
			"via DNS due to it being too long. Valid lengths are between "+
			"1 and 63 bytes.", service.Service)
	}

	for _, tag := range service.Tags {
		if InvalidDnsRe.MatchString(tag) {
			a.logger.Printf("[DEBUG] agent: Service tag %q will not be discoverable "+
				"via DNS due to invalid characters. Valid characters include "+
				"all alpha-numerics and dashes.", tag)
		} else if len(tag) > MaxDNSLabelLength {
			a.logger.Printf("[DEBUG] agent: Service tag %q will not be discoverable "+
				"via DNS due to it being too long. Valid lengths are between "+
				"1 and 63 bytes.", tag)
		}
	}

	a.PauseSync()
	defer a.ResumeSync()

	snap := a.snapshotCheckState()
	defer a.restoreCheckState(snap)

	a.State.AddService(service, token)

	if persist && !a.config.DevMode {
		if err := a.persistService(service); err != nil {
			return err
		}
	}

	for i, chkType := range chkTypes {
		checkID := string(chkType.CheckID)
		if checkID == "" {
			checkID = fmt.Sprintf("service:%s", service.ID)
			if len(chkTypes) > 1 {
				checkID += fmt.Sprintf(":%d", i+1)
			}
		}
		name := chkType.Name
		if name == "" {
			name = fmt.Sprintf("Service '%s' check", service.Service)
		}
		check := &structs.HealthCheck{
			Node:        a.config.NodeName,
			CheckID:     types.CheckID(checkID),
			Name:        name,
			Status:      api.HealthCritical,
			Notes:       chkType.Notes,
			ServiceID:   service.ID,
			ServiceName: service.Service,
			ServiceTags: service.Tags,
		}
		if chkType.Status != "" {
			check.Status = chkType.Status
		}
		if err := a.AddCheck(check, chkType, persist, token); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) RemoveService(serviceID string, persist bool) error {

	if serviceID == "" {
		return fmt.Errorf("ServiceID missing")
	}

	if err := a.State.RemoveService(serviceID); err != nil {
		a.logger.Printf("[WARN] agent: Failed to deregister service %q: %s", serviceID, err)
		return nil
	}

	if persist {
		if err := a.purgeService(serviceID); err != nil {
			return err
		}
	}

	for checkID, check := range a.State.Checks() {
		if check.ServiceID != serviceID {
			continue
		}
		if err := a.RemoveCheck(checkID, persist); err != nil {
			return err
		}
	}

	log.Printf("[DEBUG] agent: removed service %q", serviceID)
	return nil
}

func (a *Agent) AddCheck(check *structs.HealthCheck, chkType *structs.CheckType, persist bool, token string) error {
	if check.CheckID == "" {
		return fmt.Errorf("CheckID missing")
	}

	if chkType != nil {
		if err := chkType.Validate(); err != nil {
			return fmt.Errorf("Check is not valid: %v", err)
		}

		if chkType.IsScript() && !a.config.EnableScriptChecks {
			return fmt.Errorf("Scripts are disabled on this agent; to enable, configure 'enable_script_checks' to true")
		}
	}

	if check.ServiceID != "" {
		s := a.State.Service(check.ServiceID)
		if s == nil {
			return fmt.Errorf("ServiceID %q does not exist", check.ServiceID)
		}
		check.ServiceName = s.Service
		check.ServiceTags = s.Tags
	}

	a.checkLock.Lock()
	defer a.checkLock.Unlock()

	if chkType != nil {
		switch {

		case chkType.IsTTL():
			if existing, ok := a.checkTTLs[check.CheckID]; ok {
				existing.Stop()
				delete(a.checkTTLs, check.CheckID)
			}

			ttl := &checks.CheckTTL{
				Notify:  a.State,
				CheckID: check.CheckID,
				TTL:     chkType.TTL,
				Logger:  a.logger,
			}

			if err := a.loadCheckState(check); err != nil {
				a.logger.Printf("[WARN] agent: failed restoring state for check %q: %s",
					check.CheckID, err)
			}

			ttl.Start()
			a.checkTTLs[check.CheckID] = ttl

		case chkType.IsHTTP():
			if existing, ok := a.checkHTTPs[check.CheckID]; ok {
				existing.Stop()
				delete(a.checkHTTPs, check.CheckID)
			}
			if chkType.Interval < checks.MinInterval {
				a.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has interval below minimum of %v",
					check.CheckID, checks.MinInterval))
				chkType.Interval = checks.MinInterval
			}

			tlsClientConfig, err := a.setupTLSClientConfig(chkType.TLSSkipVerify)
			if err != nil {
				return fmt.Errorf("Failed to set up TLS: %v", err)
			}

			http := &checks.CheckHTTP{
				Notify:          a.State,
				CheckID:         check.CheckID,
				HTTP:            chkType.HTTP,
				Header:          chkType.Header,
				Method:          chkType.Method,
				Interval:        chkType.Interval,
				Timeout:         chkType.Timeout,
				Logger:          a.logger,
				TLSClientConfig: tlsClientConfig,
			}
			http.Start()
			a.checkHTTPs[check.CheckID] = http

		case chkType.IsTCP():
			if existing, ok := a.checkTCPs[check.CheckID]; ok {
				existing.Stop()
				delete(a.checkTCPs, check.CheckID)
			}
			if chkType.Interval < checks.MinInterval {
				a.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has interval below minimum of %v",
					check.CheckID, checks.MinInterval))
				chkType.Interval = checks.MinInterval
			}

			tcp := &checks.CheckTCP{
				Notify:   a.State,
				CheckID:  check.CheckID,
				TCP:      chkType.TCP,
				Interval: chkType.Interval,
				Timeout:  chkType.Timeout,
				Logger:   a.logger,
			}
			tcp.Start()
			a.checkTCPs[check.CheckID] = tcp

		case chkType.IsGRPC():
			if existing, ok := a.checkGRPCs[check.CheckID]; ok {
				existing.Stop()
				delete(a.checkGRPCs, check.CheckID)
			}
			if chkType.Interval < checks.MinInterval {
				a.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has interval below minimum of %v",
					check.CheckID, checks.MinInterval))
				chkType.Interval = checks.MinInterval
			}

			var tlsClientConfig *tls.Config
			if chkType.GRPCUseTLS {
				var err error
				tlsClientConfig, err = a.setupTLSClientConfig(chkType.TLSSkipVerify)
				if err != nil {
					return fmt.Errorf("Failed to set up TLS: %v", err)
				}
			}

			grpc := &checks.CheckGRPC{
				Notify:          a.State,
				CheckID:         check.CheckID,
				GRPC:            chkType.GRPC,
				Interval:        chkType.Interval,
				Timeout:         chkType.Timeout,
				Logger:          a.logger,
				TLSClientConfig: tlsClientConfig,
			}
			grpc.Start()
			a.checkGRPCs[check.CheckID] = grpc

		case chkType.IsDocker():
			if existing, ok := a.checkDockers[check.CheckID]; ok {
				existing.Stop()
				delete(a.checkDockers, check.CheckID)
			}
			if chkType.Interval < checks.MinInterval {
				a.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has interval below minimum of %v",
					check.CheckID, checks.MinInterval))
				chkType.Interval = checks.MinInterval
			}
			if chkType.Script != "" {
				a.logger.Printf("[WARN] agent: check %q has the 'script' field, which has been deprecated "+
					"and replaced with the 'args' field. See https://www.consul.io/docs/agent/checks.html",
					check.CheckID)
			}

			if a.dockerClient == nil {
				dc, err := checks.NewDockerClient(os.Getenv("DOCKER_HOST"), checks.BufSize)
				if err != nil {
					a.logger.Printf("[ERR] agent: error creating docker client: %s", err)
					return err
				}
				a.logger.Printf("[DEBUG] agent: created docker client for %s", dc.Host())
				a.dockerClient = dc
			}

			dockerCheck := &checks.CheckDocker{
				Notify:            a.State,
				CheckID:           check.CheckID,
				DockerContainerID: chkType.DockerContainerID,
				Shell:             chkType.Shell,
				Script:            chkType.Script,
				ScriptArgs:        chkType.ScriptArgs,
				Interval:          chkType.Interval,
				Logger:            a.logger,
				Client:            a.dockerClient,
			}
			if prev := a.checkDockers[check.CheckID]; prev != nil {
				prev.Stop()
			}
			dockerCheck.Start()
			a.checkDockers[check.CheckID] = dockerCheck

		case chkType.IsMonitor():
			if existing, ok := a.checkMonitors[check.CheckID]; ok {
				existing.Stop()
				delete(a.checkMonitors, check.CheckID)
			}
			if chkType.Interval < checks.MinInterval {
				a.logger.Printf("[WARN] agent: check '%s' has interval below minimum of %v",
					check.CheckID, checks.MinInterval)
				chkType.Interval = checks.MinInterval
			}
			if chkType.Script != "" {
				a.logger.Printf("[WARN] agent: check %q has the 'script' field, which has been deprecated "+
					"and replaced with the 'args' field. See https://www.consul.io/docs/agent/checks.html",
					check.CheckID)
			}

			monitor := &checks.CheckMonitor{
				Notify:     a.State,
				CheckID:    check.CheckID,
				Script:     chkType.Script,
				ScriptArgs: chkType.ScriptArgs,
				Interval:   chkType.Interval,
				Timeout:    chkType.Timeout,
				Logger:     a.logger,
			}
			monitor.Start()
			a.checkMonitors[check.CheckID] = monitor

		default:
			return fmt.Errorf("Check type is not valid")
		}

		if chkType.DeregisterCriticalServiceAfter > 0 {
			timeout := chkType.DeregisterCriticalServiceAfter
			if timeout < a.config.CheckDeregisterIntervalMin {
				timeout = a.config.CheckDeregisterIntervalMin
				a.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has deregister interval below minimum of %v",
					check.CheckID, a.config.CheckDeregisterIntervalMin))
			}
			a.checkReapAfter[check.CheckID] = timeout
		} else {
			delete(a.checkReapAfter, check.CheckID)
		}
	}

	err := a.State.AddCheck(check, token)
	if err != nil {
		a.cancelCheckMonitors(check.CheckID)
		return err
	}

	if persist && !a.config.DevMode {
		return a.persistCheck(check, chkType)
	}

	return nil
}

func (a *Agent) setupTLSClientConfig(skipVerify bool) (tlsClientConfig *tls.Config, err error) {

	tlsConfig := &api.TLSConfig{
		InsecureSkipVerify: skipVerify,
	}
	if a.config.EnableAgentTLSForChecks {
		tlsConfig.Address = a.config.ServerName
		tlsConfig.KeyFile = a.config.KeyFile
		tlsConfig.CertFile = a.config.CertFile
		tlsConfig.CAFile = a.config.CAFile
		tlsConfig.CAPath = a.config.CAPath
	}
	tlsClientConfig, err = api.SetupTLSConfig(tlsConfig)
	return
}

func (a *Agent) RemoveCheck(checkID types.CheckID, persist bool) error {

	if checkID == "" {
		return fmt.Errorf("CheckID missing")
	}

	a.State.RemoveCheck(checkID)

	a.checkLock.Lock()
	defer a.checkLock.Unlock()

	a.cancelCheckMonitors(checkID)

	if persist {
		if err := a.purgeCheck(checkID); err != nil {
			return err
		}
		if err := a.purgeCheckState(checkID); err != nil {
			return err
		}
	}
	a.logger.Printf("[DEBUG] agent: removed check %q", checkID)
	return nil
}

func (a *Agent) cancelCheckMonitors(checkID types.CheckID) {

	delete(a.checkReapAfter, checkID)
	if check, ok := a.checkMonitors[checkID]; ok {
		check.Stop()
		delete(a.checkMonitors, checkID)
	}
	if check, ok := a.checkHTTPs[checkID]; ok {
		check.Stop()
		delete(a.checkHTTPs, checkID)
	}
	if check, ok := a.checkTCPs[checkID]; ok {
		check.Stop()
		delete(a.checkTCPs, checkID)
	}
	if check, ok := a.checkGRPCs[checkID]; ok {
		check.Stop()
		delete(a.checkGRPCs, checkID)
	}
	if check, ok := a.checkTTLs[checkID]; ok {
		check.Stop()
		delete(a.checkTTLs, checkID)
	}
	if check, ok := a.checkDockers[checkID]; ok {
		check.Stop()
		delete(a.checkDockers, checkID)
	}
}

func (a *Agent) updateTTLCheck(checkID types.CheckID, status, output string) error {
	a.checkLock.Lock()
	defer a.checkLock.Unlock()

	check, ok := a.checkTTLs[checkID]
	if !ok {
		return fmt.Errorf("CheckID %q does not have associated TTL", checkID)
	}

	check.SetStatus(status, output)

	if a.config.DevMode {
		return nil
	}

	if err := a.persistCheckState(check, status, output); err != nil {
		return fmt.Errorf("failed persisting state for check %q: %s", checkID, err)
	}

	return nil
}

func (a *Agent) persistCheckState(check *checks.CheckTTL, status, output string) error {

	state := persistedCheckState{
		CheckID: check.CheckID,
		Status:  status,
		Output:  output,
		Expires: time.Now().Add(check.TTL).Unix(),
	}

	buf, err := json.Marshal(state)
	if err != nil {
		return err
	}

	dir := filepath.Join(a.config.DataDir, checkStateDir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed creating check state dir %q: %s", dir, err)
	}

	file := filepath.Join(dir, checkIDHash(check.CheckID))

	tempFile := file + ".tmp"

	if err := ioutil.WriteFile(tempFile, buf, 0600); err != nil {
		return fmt.Errorf("failed writing temp file %q: %s", tempFile, err)
	}
	if err := os.Rename(tempFile, file); err != nil {
		return fmt.Errorf("failed to rename temp file from %q to %q: %s", tempFile, file, err)
	}

	return nil
}

func (a *Agent) loadCheckState(check *structs.HealthCheck) error {

	file := filepath.Join(a.config.DataDir, checkStateDir, checkIDHash(check.CheckID))
	buf, err := ioutil.ReadFile(file)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed reading file %q: %s", file, err)
	}

	var p persistedCheckState
	if err := json.Unmarshal(buf, &p); err != nil {
		a.logger.Printf("[ERR] agent: failed decoding check state: %s", err)
		return a.purgeCheckState(check.CheckID)
	}

	if time.Now().Unix() >= p.Expires {
		a.logger.Printf("[DEBUG] agent: check state expired for %q, not restoring", check.CheckID)
		return a.purgeCheckState(check.CheckID)
	}

	check.Output = p.Output
	check.Status = p.Status
	return nil
}

func (a *Agent) purgeCheckState(checkID types.CheckID) error {
	file := filepath.Join(a.config.DataDir, checkStateDir, checkIDHash(checkID))
	err := os.Remove(file)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (a *Agent) GossipEncrypted() bool {
	return a.delegate.Encrypted()
}

func (a *Agent) Stats() map[string]map[string]string {
	stats := a.delegate.Stats()
	stats["agent"] = map[string]string{
		"check_monitors": strconv.Itoa(len(a.checkMonitors)),
		"check_ttls":     strconv.Itoa(len(a.checkTTLs)),
	}
	for k, v := range a.State.Stats() {
		stats["agent"][k] = v
	}

	revision := a.config.Revision
	if len(revision) > 8 {
		revision = revision[:8]
	}
	stats["build"] = map[string]string{
		"revision":   revision,
		"version":    a.config.Version,
		"prerelease": a.config.VersionPrerelease,
	}
	return stats
}

func (a *Agent) storePid() error {

	pidPath := a.config.PidFile
	if pidPath == "" {
		return nil
	}

	pidFile, err := os.OpenFile(pidPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("Could not open pid file: %v", err)
	}
	defer pidFile.Close()

	pid := os.Getpid()
	_, err = pidFile.WriteString(fmt.Sprintf("%d", pid))
	if err != nil {
		return fmt.Errorf("Could not write to pid file: %s", err)
	}
	return nil
}

func (a *Agent) deletePid() error {

	pidPath := a.config.PidFile
	if pidPath == "" {
		return nil
	}

	stat, err := os.Stat(pidPath)
	if err != nil {
		return fmt.Errorf("Could not remove pid file: %s", err)
	}

	if stat.IsDir() {
		return fmt.Errorf("Specified pid file path is directory")
	}

	err = os.Remove(pidPath)
	if err != nil {
		return fmt.Errorf("Could not remove pid file: %s", err)
	}
	return nil
}

func (a *Agent) loadServices(conf *config.RuntimeConfig) error {

	for _, service := range conf.Services {
		ns := service.NodeService()
		chkTypes, err := service.CheckTypes()
		if err != nil {
			return fmt.Errorf("Failed to validate checks for service %q: %v", service.Name, err)
		}
		if err := a.AddService(ns, chkTypes, false, service.Token); err != nil {
			return fmt.Errorf("Failed to register service %q: %v", service.Name, err)
		}
	}

	svcDir := filepath.Join(a.config.DataDir, servicesDir)
	files, err := ioutil.ReadDir(svcDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("Failed reading services dir %q: %s", svcDir, err)
	}
	for _, fi := range files {

		if fi.IsDir() {
			continue
		}

		if strings.HasSuffix(fi.Name(), "tmp") {
			a.logger.Printf("[WARN] agent: Ignoring temporary service file %v", fi.Name())
			continue
		}

		file := filepath.Join(svcDir, fi.Name())
		fh, err := os.Open(file)
		if err != nil {
			return fmt.Errorf("failed opening service file %q: %s", file, err)
		}

		buf, err := ioutil.ReadAll(fh)
		fh.Close()
		if err != nil {
			return fmt.Errorf("failed reading service file %q: %s", file, err)
		}

		var p persistedService
		if err := json.Unmarshal(buf, &p); err != nil {

			if err := json.Unmarshal(buf, &p.Service); err != nil {
				a.logger.Printf("[ERR] agent: Failed decoding service file %q: %s", file, err)
				continue
			}
		}
		serviceID := p.Service.ID

		if a.State.Service(serviceID) != nil {

			a.logger.Printf("[DEBUG] agent: service %q exists, not restoring from %q",
				serviceID, file)
			if err := a.purgeService(serviceID); err != nil {
				return fmt.Errorf("failed purging service %q: %s", serviceID, err)
			}
		} else {
			a.logger.Printf("[DEBUG] agent: restored service definition %q from %q",
				serviceID, file)
			if err := a.AddService(p.Service, nil, false, p.Token); err != nil {
				return fmt.Errorf("failed adding service %q: %s", serviceID, err)
			}
		}
	}

	return nil
}

func (a *Agent) unloadServices() error {
	for id := range a.State.Services() {
		if err := a.RemoveService(id, false); err != nil {
			return fmt.Errorf("Failed deregistering service '%s': %v", id, err)
		}
	}
	return nil
}

func (a *Agent) loadChecks(conf *config.RuntimeConfig) error {

	for _, check := range conf.Checks {
		health := check.HealthCheck(conf.NodeName)
		chkType := check.CheckType()
		if err := a.AddCheck(health, chkType, false, check.Token); err != nil {
			return fmt.Errorf("Failed to register check '%s': %v %v", check.Name, err, check)
		}
	}

	checkDir := filepath.Join(a.config.DataDir, checksDir)
	files, err := ioutil.ReadDir(checkDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("Failed reading checks dir %q: %s", checkDir, err)
	}
	for _, fi := range files {

		if fi.IsDir() {
			continue
		}

		file := filepath.Join(checkDir, fi.Name())
		fh, err := os.Open(file)
		if err != nil {
			return fmt.Errorf("Failed opening check file %q: %s", file, err)
		}

		buf, err := ioutil.ReadAll(fh)
		fh.Close()
		if err != nil {
			return fmt.Errorf("failed reading check file %q: %s", file, err)
		}

		var p persistedCheck
		if err := json.Unmarshal(buf, &p); err != nil {
			a.logger.Printf("[ERR] agent: Failed decoding check file %q: %s", file, err)
			continue
		}
		checkID := p.Check.CheckID

		if a.State.Check(checkID) != nil {

			a.logger.Printf("[DEBUG] agent: check %q exists, not restoring from %q",
				checkID, file)
			if err := a.purgeCheck(checkID); err != nil {
				return fmt.Errorf("Failed purging check %q: %s", checkID, err)
			}
		} else {

			p.Check.Status = api.HealthCritical

			if err := a.AddCheck(p.Check, p.ChkType, false, p.Token); err != nil {

				a.logger.Printf("[WARN] agent: Failed to restore check %q: %s",
					checkID, err)
				if err := a.purgeCheck(checkID); err != nil {
					return fmt.Errorf("Failed purging check %q: %s", checkID, err)
				}
			}
			a.logger.Printf("[DEBUG] agent: restored health check %q from %q",
				p.Check.CheckID, file)
		}
	}

	return nil
}

func (a *Agent) unloadChecks() error {
	for id := range a.State.Checks() {
		if err := a.RemoveCheck(id, false); err != nil {
			return fmt.Errorf("Failed deregistering check '%s': %s", id, err)
		}
	}
	return nil
}

func (a *Agent) snapshotCheckState() map[types.CheckID]*structs.HealthCheck {
	return a.State.Checks()
}

func (a *Agent) restoreCheckState(snap map[types.CheckID]*structs.HealthCheck) {
	for id, check := range snap {
		a.State.UpdateCheck(id, check.Status, check.Output)
	}
}

func (a *Agent) loadMetadata(conf *config.RuntimeConfig) error {
	meta := map[string]string{}
	for k, v := range conf.NodeMeta {
		meta[k] = v
	}
	meta[structs.MetaSegmentKey] = conf.SegmentName
	return a.State.LoadMetadata(meta)
}

func (a *Agent) unloadMetadata() {
	a.State.UnloadMetadata()
}

func serviceMaintCheckID(serviceID string) types.CheckID {
	return types.CheckID(structs.ServiceMaintPrefix + serviceID)
}

func (a *Agent) EnableServiceMaintenance(serviceID, reason, token string) error {
	service, ok := a.State.Services()[serviceID]
	if !ok {
		return fmt.Errorf("No service registered with ID %q", serviceID)
	}

	checkID := serviceMaintCheckID(serviceID)
	if _, ok := a.State.Checks()[checkID]; ok {
		return nil
	}

	if reason == "" {
		reason = defaultServiceMaintReason
	}

	check := &structs.HealthCheck{
		Node:        a.config.NodeName,
		CheckID:     checkID,
		Name:        "Service Maintenance Mode",
		Notes:       reason,
		ServiceID:   service.ID,
		ServiceName: service.Service,
		Status:      api.HealthCritical,
	}
	a.AddCheck(check, nil, true, token)
	a.logger.Printf("[INFO] agent: Service %q entered maintenance mode", serviceID)

	return nil
}

func (a *Agent) DisableServiceMaintenance(serviceID string) error {
	if _, ok := a.State.Services()[serviceID]; !ok {
		return fmt.Errorf("No service registered with ID %q", serviceID)
	}

	checkID := serviceMaintCheckID(serviceID)
	if _, ok := a.State.Checks()[checkID]; !ok {
		return nil
	}

	a.RemoveCheck(checkID, true)
	a.logger.Printf("[INFO] agent: Service %q left maintenance mode", serviceID)

	return nil
}

func (a *Agent) EnableNodeMaintenance(reason, token string) {

	if _, ok := a.State.Checks()[structs.NodeMaint]; ok {
		return
	}

	if reason == "" {
		reason = defaultNodeMaintReason
	}

	check := &structs.HealthCheck{
		Node:    a.config.NodeName,
		CheckID: structs.NodeMaint,
		Name:    "Node Maintenance Mode",
		Notes:   reason,
		Status:  api.HealthCritical,
	}
	a.AddCheck(check, nil, true, token)
	a.logger.Printf("[INFO] agent: Node entered maintenance mode")
}

func (a *Agent) DisableNodeMaintenance() {
	if _, ok := a.State.Checks()[structs.NodeMaint]; !ok {
		return
	}
	a.RemoveCheck(structs.NodeMaint, true)
	a.logger.Printf("[INFO] agent: Node left maintenance mode")
}

func (a *Agent) ReloadConfig(newCfg *config.RuntimeConfig) error {

	a.PauseSync()
	defer a.ResumeSync()

	snap := a.snapshotCheckState()
	defer a.restoreCheckState(snap)

	if err := a.unloadServices(); err != nil {
		return fmt.Errorf("Failed unloading services: %s", err)
	}
	if err := a.unloadChecks(); err != nil {
		return fmt.Errorf("Failed unloading checks: %s", err)
	}
	a.unloadMetadata()

	if err := a.loadServices(newCfg); err != nil {
		return fmt.Errorf("Failed reloading services: %s", err)
	}
	if err := a.loadChecks(newCfg); err != nil {
		return fmt.Errorf("Failed reloading checks: %s", err)
	}
	if err := a.loadMetadata(newCfg); err != nil {
		return fmt.Errorf("Failed reloading metadata: %s", err)
	}

	if err := a.reloadWatches(newCfg); err != nil {
		return fmt.Errorf("Failed reloading watches: %v", err)
	}

	metrics.UpdateFilter(newCfg.TelemetryAllowedPrefixes, newCfg.TelemetryBlockedPrefixes)

	a.State.SetDiscardCheckOutput(newCfg.DiscardCheckOutput)

	return nil
}
