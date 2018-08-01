/*
memberlist is a library that manages cluster
membership and member failure detection using a gossip based protocol.

The use cases for such a library are far-reaching: all distributed systems
require membership, and memberlist is a re-usable solution to managing
cluster membership and node failure detection.

memberlist is eventually consistent but converges quickly on average.
The speed at which it converges can be heavily tuned via various knobs
on the protocol. Node failures are detected and network partitions are partially
tolerated by attempting to communicate to potentially dead nodes through
multiple routes.
*/
package memberlist

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	multierror "github.com/as/consulrun/hashicorp/go-multierror"
	sockaddr "github.com/as/consulrun/hashicorp/go-sockaddr"
	"github.com/miekg/dns"
)

type Memberlist struct {
	sequenceNum uint32 // Local sequence number
	incarnation uint32 // Local incarnation number
	numNodes    uint32 // Number of known nodes (estimate)

	config         *Config
	shutdown       int32 // Used as an atomic boolean value
	shutdownCh     chan struct{}
	leave          int32 // Used as an atomic boolean value
	leaveBroadcast chan struct{}

	shutdownLock sync.Mutex // Serializes calls to Shutdown
	leaveLock    sync.Mutex // Serializes calls to Leave

	transport Transport
	handoff   chan msgHandoff

	nodeLock   sync.RWMutex
	nodes      []*nodeState          // Known nodes
	nodeMap    map[string]*nodeState // Maps Addr.String() -> NodeState
	nodeTimers map[string]*suspicion // Maps Addr.String() -> suspicion timer
	awareness  *awareness

	tickerLock sync.Mutex
	tickers    []*time.Ticker
	stopTick   chan struct{}
	probeIndex int

	ackLock     sync.Mutex
	ackHandlers map[uint32]*ackHandler

	broadcasts *TransmitLimitedQueue

	logger *log.Logger
}

func newMemberlist(conf *Config) (*Memberlist, error) {
	if conf.ProtocolVersion < ProtocolVersionMin {
		return nil, fmt.Errorf("Protocol version '%d' too low. Must be in range: [%d, %d]",
			conf.ProtocolVersion, ProtocolVersionMin, ProtocolVersionMax)
	} else if conf.ProtocolVersion > ProtocolVersionMax {
		return nil, fmt.Errorf("Protocol version '%d' too high. Must be in range: [%d, %d]",
			conf.ProtocolVersion, ProtocolVersionMin, ProtocolVersionMax)
	}

	if len(conf.SecretKey) > 0 {
		if conf.Keyring == nil {
			keyring, err := NewKeyring(nil, conf.SecretKey)
			if err != nil {
				return nil, err
			}
			conf.Keyring = keyring
		} else {
			if err := conf.Keyring.AddKey(conf.SecretKey); err != nil {
				return nil, err
			}
			if err := conf.Keyring.UseKey(conf.SecretKey); err != nil {
				return nil, err
			}
		}
	}

	if conf.LogOutput != nil && conf.Logger != nil {
		return nil, fmt.Errorf("Cannot specify both LogOutput and Logger. Please choose a single log configuration setting.")
	}

	logDest := conf.LogOutput
	if logDest == nil {
		logDest = os.Stderr
	}

	logger := conf.Logger
	if logger == nil {
		logger = log.New(logDest, "", log.LstdFlags)
	}

	transport := conf.Transport
	if transport == nil {
		nc := &NetTransportConfig{
			BindAddrs: []string{conf.BindAddr},
			BindPort:  conf.BindPort,
			Logger:    logger,
		}

		makeNetRetry := func(limit int) (*NetTransport, error) {
			var err error
			for try := 0; try < limit; try++ {
				var nt *NetTransport
				if nt, err = NewNetTransport(nc); err == nil {
					return nt, nil
				}
				if strings.Contains(err.Error(), "address already in use") {
					logger.Printf("[DEBUG] memberlist: Got bind error: %v", err)
					continue
				}
			}

			return nil, fmt.Errorf("failed to obtain an address: %v", err)
		}

		limit := 1
		if conf.BindPort == 0 {
			limit = 10
		}

		nt, err := makeNetRetry(limit)
		if err != nil {
			return nil, fmt.Errorf("Could not set up network transport: %v", err)
		}
		if conf.BindPort == 0 {
			port := nt.GetAutoBindPort()
			conf.BindPort = port
			conf.AdvertisePort = port
			logger.Printf("[DEBUG] memberlist: Using dynamic bind port %d", port)
		}
		transport = nt
	}

	m := &Memberlist{
		config:         conf,
		shutdownCh:     make(chan struct{}),
		leaveBroadcast: make(chan struct{}, 1),
		transport:      transport,
		handoff:        make(chan msgHandoff, conf.HandoffQueueDepth),
		nodeMap:        make(map[string]*nodeState),
		nodeTimers:     make(map[string]*suspicion),
		awareness:      newAwareness(conf.AwarenessMaxMultiplier),
		ackHandlers:    make(map[uint32]*ackHandler),
		broadcasts:     &TransmitLimitedQueue{RetransmitMult: conf.RetransmitMult},
		logger:         logger,
	}
	m.broadcasts.NumNodes = func() int {
		return m.estNumNodes()
	}
	go m.streamListen()
	go m.packetListen()
	go m.packetHandler()
	return m, nil
}

func Create(conf *Config) (*Memberlist, error) {
	m, err := newMemberlist(conf)
	if err != nil {
		return nil, err
	}
	if err := m.setAlive(); err != nil {
		m.Shutdown()
		return nil, err
	}
	m.schedule()
	return m, nil
}

//
func (m *Memberlist) Join(existing []string) (int, error) {
	numSuccess := 0
	var errs error
	for _, exist := range existing {
		addrs, err := m.resolveAddr(exist)
		if err != nil {
			err = fmt.Errorf("Failed to resolve %s: %v", exist, err)
			errs = multierror.Append(errs, err)
			m.logger.Printf("[WARN] memberlist: %v", err)
			continue
		}

		for _, addr := range addrs {
			hp := joinHostPort(addr.ip.String(), addr.port)
			if err := m.pushPullNode(hp, true); err != nil {
				err = fmt.Errorf("Failed to join %s: %v", addr.ip, err)
				errs = multierror.Append(errs, err)
				m.logger.Printf("[DEBUG] memberlist: %v", err)
				continue
			}
			numSuccess++
		}

	}
	if numSuccess > 0 {
		errs = nil
	}
	return numSuccess, errs
}

type ipPort struct {
	ip   net.IP
	port uint16
}

func (m *Memberlist) tcpLookupIP(host string, defaultPort uint16) ([]ipPort, error) {

	if !strings.Contains(host, ".") {
		return nil, nil
	}

	dn := host
	if dn[len(dn)-1] != '.' {
		dn = dn + "."
	}

	cc, err := dns.ClientConfigFromFile(m.config.DNSConfigPath)
	if err != nil {
		return nil, err
	}
	if len(cc.Servers) > 0 {

		server := cc.Servers[0]
		if !hasPort(server) {
			server = net.JoinHostPort(server, cc.Port)
		}

		c := new(dns.Client)
		c.Net = "tcp"
		msg := new(dns.Msg)
		msg.SetQuestion(dn, dns.TypeANY)
		in, _, err := c.Exchange(msg, server)
		if err != nil {
			return nil, err
		}

		var ips []ipPort
		for _, r := range in.Answer {
			switch rr := r.(type) {
			case (*dns.A):
				ips = append(ips, ipPort{rr.A, defaultPort})
			case (*dns.AAAA):
				ips = append(ips, ipPort{rr.AAAA, defaultPort})
			case (*dns.CNAME):
				m.logger.Printf("[DEBUG] memberlist: Ignoring CNAME RR in TCP-first answer for '%s'", host)
			}
		}
		return ips, nil
	}

	return nil, nil
}

func (m *Memberlist) resolveAddr(hostStr string) ([]ipPort, error) {

	hostStr = ensurePort(hostStr, m.config.BindPort)
	host, sport, err := net.SplitHostPort(hostStr)
	if err != nil {
		return nil, err
	}
	lport, err := strconv.ParseUint(sport, 10, 16)
	if err != nil {
		return nil, err
	}
	port := uint16(lport)

	if ip := net.ParseIP(host); ip != nil {
		return []ipPort{{ip, port}}, nil
	}

	ips, err := m.tcpLookupIP(host, port)
	if err != nil {
		m.logger.Printf("[DEBUG] memberlist: TCP-first lookup failed for '%s', falling back to UDP: %s", hostStr, err)
	}
	if len(ips) > 0 {
		return ips, nil
	}

	ans, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	ips = make([]ipPort, 0, len(ans))
	for _, ip := range ans {
		ips = append(ips, ipPort{ip, port})
	}
	return ips, nil
}

func (m *Memberlist) setAlive() error {

	addr, port, err := m.transport.FinalAdvertiseAddr(
		m.config.AdvertiseAddr, m.config.AdvertisePort)
	if err != nil {
		return fmt.Errorf("Failed to get final advertise address: %v", err)
	}

	ipAddr, err := sockaddr.NewIPAddr(addr.String())
	if err != nil {
		return fmt.Errorf("Failed to parse interface addresses: %v", err)
	}
	ifAddrs := []sockaddr.IfAddr{
		{
			SockAddr: ipAddr,
		},
	}
	_, publicIfs, err := sockaddr.IfByRFC("6890", ifAddrs)
	if len(publicIfs) > 0 && !m.config.EncryptionEnabled() {
		m.logger.Printf("[WARN] memberlist: Binding to public address without encryption!")
	}

	var meta []byte
	if m.config.Delegate != nil {
		meta = m.config.Delegate.NodeMeta(MetaMaxSize)
		if len(meta) > MetaMaxSize {
			panic("Node meta data provided is longer than the limit")
		}
	}

	a := alive{
		Incarnation: m.nextIncarnation(),
		Node:        m.config.Name,
		Addr:        addr,
		Port:        uint16(port),
		Meta:        meta,
		Vsn: []uint8{
			ProtocolVersionMin, ProtocolVersionMax, m.config.ProtocolVersion,
			m.config.DelegateProtocolMin, m.config.DelegateProtocolMax,
			m.config.DelegateProtocolVersion,
		},
	}
	m.aliveNode(&a, nil, true)
	return nil
}

func (m *Memberlist) LocalNode() *Node {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()
	state := m.nodeMap[m.config.Name]
	return &state.Node
}

func (m *Memberlist) UpdateNode(timeout time.Duration) error {

	var meta []byte
	if m.config.Delegate != nil {
		meta = m.config.Delegate.NodeMeta(MetaMaxSize)
		if len(meta) > MetaMaxSize {
			panic("Node meta data provided is longer than the limit")
		}
	}

	m.nodeLock.RLock()
	state := m.nodeMap[m.config.Name]
	m.nodeLock.RUnlock()

	a := alive{
		Incarnation: m.nextIncarnation(),
		Node:        m.config.Name,
		Addr:        state.Addr,
		Port:        state.Port,
		Meta:        meta,
		Vsn: []uint8{
			ProtocolVersionMin, ProtocolVersionMax, m.config.ProtocolVersion,
			m.config.DelegateProtocolMin, m.config.DelegateProtocolMax,
			m.config.DelegateProtocolVersion,
		},
	}
	notifyCh := make(chan struct{})
	m.aliveNode(&a, notifyCh, true)

	if m.anyAlive() {
		var timeoutCh <-chan time.Time
		if timeout > 0 {
			timeoutCh = time.After(timeout)
		}
		select {
		case <-notifyCh:
		case <-timeoutCh:
			return fmt.Errorf("timeout waiting for update broadcast")
		}
	}
	return nil
}

func (m *Memberlist) SendTo(to net.Addr, msg []byte) error {

	buf := make([]byte, 1, len(msg)+1)
	buf[0] = byte(userMsg)
	buf = append(buf, msg...)

	return m.rawSendMsgPacket(to.String(), nil, buf)
}

func (m *Memberlist) SendToUDP(to *Node, msg []byte) error {
	return m.SendBestEffort(to, msg)
}

func (m *Memberlist) SendToTCP(to *Node, msg []byte) error {
	return m.SendReliable(to, msg)
}

func (m *Memberlist) SendBestEffort(to *Node, msg []byte) error {

	buf := make([]byte, 1, len(msg)+1)
	buf[0] = byte(userMsg)
	buf = append(buf, msg...)

	return m.rawSendMsgPacket(to.Address(), to, buf)
}

func (m *Memberlist) SendReliable(to *Node, msg []byte) error {
	return m.sendUserMsg(to.Address(), msg)
}

func (m *Memberlist) Members() []*Node {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	nodes := make([]*Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		if n.State != stateDead {
			nodes = append(nodes, &n.Node)
		}
	}

	return nodes
}

func (m *Memberlist) NumMembers() (alive int) {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()

	for _, n := range m.nodes {
		if n.State != stateDead {
			alive++
		}
	}

	return
}

//
//
func (m *Memberlist) Leave(timeout time.Duration) error {
	m.leaveLock.Lock()
	defer m.leaveLock.Unlock()

	if m.hasShutdown() {
		panic("leave after shutdown")
	}

	if !m.hasLeft() {
		atomic.StoreInt32(&m.leave, 1)

		m.nodeLock.Lock()
		state, ok := m.nodeMap[m.config.Name]
		m.nodeLock.Unlock()
		if !ok {
			m.logger.Printf("[WARN] memberlist: Leave but we're not in the node map.")
			return nil
		}

		d := dead{
			Incarnation: state.Incarnation,
			Node:        state.Name,
		}
		m.deadNode(&d)

		if m.anyAlive() {
			var timeoutCh <-chan time.Time
			if timeout > 0 {
				timeoutCh = time.After(timeout)
			}
			select {
			case <-m.leaveBroadcast:
			case <-timeoutCh:
				return fmt.Errorf("timeout waiting for leave broadcast")
			}
		}
	}

	return nil
}

func (m *Memberlist) anyAlive() bool {
	m.nodeLock.RLock()
	defer m.nodeLock.RUnlock()
	for _, n := range m.nodes {
		if n.State != stateDead && n.Name != m.config.Name {
			return true
		}
	}
	return false
}

func (m *Memberlist) GetHealthScore() int {
	return m.awareness.GetHealthScore()
}

func (m *Memberlist) ProtocolVersion() uint8 {

	return m.config.ProtocolVersion
}

//
func (m *Memberlist) Shutdown() error {
	m.shutdownLock.Lock()
	defer m.shutdownLock.Unlock()

	if m.hasShutdown() {
		return nil
	}

	m.transport.Shutdown()

	atomic.StoreInt32(&m.shutdown, 1)
	close(m.shutdownCh)
	m.deschedule()
	return nil
}

func (m *Memberlist) hasShutdown() bool {
	return atomic.LoadInt32(&m.shutdown) == 1
}

func (m *Memberlist) hasLeft() bool {
	return atomic.LoadInt32(&m.leave) == 1
}
