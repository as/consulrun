package router

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"testing"
	"time"

	"github.com/as/consulrun/hashicorp/consul/agent/metadata"
)

var (
	localLogger    *log.Logger
	localLogBuffer *bytes.Buffer
)

func init() {
	localLogBuffer = new(bytes.Buffer)
	localLogger = log.New(localLogBuffer, "", 0)
}

func GetBufferedLogger() *log.Logger {
	return localLogger
}

type fauxConnPool struct {
	failPct float64
}

func (cp *fauxConnPool) Ping(string, net.Addr, int, bool) (bool, error) {
	var success bool
	successProb := rand.Float64()
	if successProb > cp.failPct {
		success = true
	}
	return success, nil
}

type fauxSerf struct {
	numNodes int
}

func (s *fauxSerf) NumNodes() int {
	return s.numNodes
}

func testManager() (m *Manager) {
	logger := GetBufferedLogger()
	shutdownCh := make(chan struct{})
	m = New(logger, shutdownCh, &fauxSerf{numNodes: 16384}, &fauxConnPool{})
	return m
}

func testManagerFailProb(failPct float64) (m *Manager) {
	logger := GetBufferedLogger()
	logger = log.New(os.Stderr, "", log.LstdFlags)
	shutdownCh := make(chan struct{})
	m = New(logger, shutdownCh, &fauxSerf{}, &fauxConnPool{failPct: failPct})
	return m
}

func TestManagerInternal_cycleServer(t *testing.T) {
	m := testManager()
	l := m.getServerList()

	server0 := &metadata.Server{Name: "server1"}
	server1 := &metadata.Server{Name: "server2"}
	server2 := &metadata.Server{Name: "server3"}
	l.servers = append(l.servers, server0, server1, server2)
	m.saveServerList(l)

	l = m.getServerList()
	if len(l.servers) != 3 {
		t.Fatalf("server length incorrect: %d/3", len(l.servers))
	}
	if l.servers[0] != server0 &&
		l.servers[1] != server1 &&
		l.servers[2] != server2 {
		t.Fatalf("initial server ordering not correct")
	}

	l.servers = l.cycleServer()
	if len(l.servers) != 3 {
		t.Fatalf("server length incorrect: %d/3", len(l.servers))
	}
	if l.servers[0] != server1 &&
		l.servers[1] != server2 &&
		l.servers[2] != server0 {
		t.Fatalf("server ordering after one cycle not correct")
	}

	l.servers = l.cycleServer()
	if len(l.servers) != 3 {
		t.Fatalf("server length incorrect: %d/3", len(l.servers))
	}
	if l.servers[0] != server2 &&
		l.servers[1] != server0 &&
		l.servers[2] != server1 {
		t.Fatalf("server ordering after two cycles not correct")
	}

	l.servers = l.cycleServer()
	if len(l.servers) != 3 {
		t.Fatalf("server length incorrect: %d/3", len(l.servers))
	}
	if l.servers[0] != server0 &&
		l.servers[1] != server1 &&
		l.servers[2] != server2 {
		t.Fatalf("server ordering after three cycles not correct")
	}
}

func TestManagerInternal_getServerList(t *testing.T) {
	m := testManager()
	l := m.getServerList()
	if l.servers == nil {
		t.Fatalf("serverList.servers nil")
	}

	if len(l.servers) != 0 {
		t.Fatalf("serverList.servers length not zero")
	}
}

func TestManagerInternal_New(t *testing.T) {
	m := testManager()
	if m == nil {
		t.Fatalf("Manager nil")
	}

	if m.clusterInfo == nil {
		t.Fatalf("Manager.clusterInfo nil")
	}

	if m.logger == nil {
		t.Fatalf("Manager.logger nil")
	}

	if m.shutdownCh == nil {
		t.Fatalf("Manager.shutdownCh nil")
	}
}

func TestManagerInternal_reconcileServerList(t *testing.T) {
	tests := []int{0, 1, 2, 3, 4, 5, 10, 100}
	for _, n := range tests {
		ok, err := test_reconcileServerList(n)
		if !ok {
			t.Errorf("Expected %d to pass: %v", n, err)
		}
	}
}

func test_reconcileServerList(maxServers int) (bool, error) {

	const failPct = 0.5
	m := testManagerFailProb(failPct)

	var failedServers, healthyServers []*metadata.Server
	for i := 0; i < maxServers; i++ {
		nodeName := fmt.Sprintf("s%02d", i)

		node := &metadata.Server{Name: nodeName}

		if rand.Float64() > 0.33 {
			m.AddServer(node)

			if ok, _ := m.connPoolPinger.Ping(node.Datacenter, node.Addr, node.Version, node.UseTLS); ok {

				healthyServers = append(healthyServers, node)
			} else {

				failedServers = append(failedServers, node)
			}
		} else {

			healthyServers = append(healthyServers, node)
		}
	}

	m.RebalanceServers()
	selectedServer := m.FindServer()

	var selectedServerFailed bool
	for _, s := range failedServers {
		if selectedServer.Key().Equal(s.Key()) {
			selectedServerFailed = true
			break
		}
	}

	origServers := m.getServerList()
	m.saveServerList(serverList{servers: healthyServers})

	if !selectedServerFailed && !m.reconcileServerList(&origServers) &&
		len(m.getServerList().servers) != 0 &&
		len(origServers.servers) != 0 {

		return false, fmt.Errorf("Expected reconcile to succeed: %v %d %d",
			selectedServerFailed,
			len(m.getServerList().servers),
			len(origServers.servers))
	}

	if len(m.getServerList().servers) == 0 &&
		len(origServers.servers) == 0 {

		return true, nil
	}

	resultingServerMap := make(map[metadata.Key]bool)
	for _, s := range m.getServerList().servers {
		resultingServerMap[*s.Key()] = true
	}

	for _, s := range failedServers {
		_, ok := resultingServerMap[*s.Key()]
		if ok {
			return false, fmt.Errorf("Found failed server %v in merged list %v", s, resultingServerMap)
		}
	}

	if len(healthyServers) != len(m.getServerList().servers) {
		return false, fmt.Errorf("Expected healthy map and servers to match: %d/%d", len(healthyServers), len(healthyServers))
	}

	for _, s := range healthyServers {
		_, ok := resultingServerMap[*s.Key()]
		if !ok {
			return false, fmt.Errorf("Server %v missing from healthy map after merged lists", s)
		}
	}
	return true, nil
}

func TestManagerInternal_refreshServerRebalanceTimer(t *testing.T) {
	type clusterSizes struct {
		numNodes     int
		numServers   int
		minRebalance time.Duration
	}
	clusters := []clusterSizes{
		{0, 3, 2 * time.Minute},
		{1, 0, 2 * time.Minute}, // partitioned cluster
		{1, 3, 2 * time.Minute},
		{2, 3, 2 * time.Minute},
		{100, 0, 2 * time.Minute}, // partitioned
		{100, 1, 2 * time.Minute}, // partitioned
		{100, 3, 2 * time.Minute},
		{1024, 1, 2 * time.Minute}, // partitioned
		{1024, 3, 2 * time.Minute}, // partitioned
		{1024, 5, 2 * time.Minute},
		{16384, 1, 4 * time.Minute}, // partitioned
		{16384, 2, 2 * time.Minute}, // partitioned
		{16384, 3, 2 * time.Minute}, // partitioned
		{16384, 5, 2 * time.Minute},
		{65535, 0, 2 * time.Minute}, // partitioned
		{65535, 1, 8 * time.Minute}, // partitioned
		{65535, 2, 3 * time.Minute}, // partitioned
		{65535, 3, 5 * time.Minute}, // partitioned
		{65535, 5, 3 * time.Minute}, // partitioned
		{65535, 7, 2 * time.Minute},
		{1000000, 1, 4 * time.Hour},     // partitioned
		{1000000, 2, 2 * time.Hour},     // partitioned
		{1000000, 3, 80 * time.Minute},  // partitioned
		{1000000, 5, 50 * time.Minute},  // partitioned
		{1000000, 11, 20 * time.Minute}, // partitioned
		{1000000, 19, 10 * time.Minute},
	}

	logger := log.New(os.Stderr, "", log.LstdFlags)
	shutdownCh := make(chan struct{})

	for _, s := range clusters {
		m := New(logger, shutdownCh, &fauxSerf{numNodes: s.numNodes}, &fauxConnPool{})
		for i := 0; i < s.numServers; i++ {
			nodeName := fmt.Sprintf("s%02d", i)
			m.AddServer(&metadata.Server{Name: nodeName})
		}

		d := m.refreshServerRebalanceTimer()
		if d < s.minRebalance {
			t.Errorf("duration too short for cluster of size %d and %d servers (%s < %s)", s.numNodes, s.numServers, d, s.minRebalance)
		}
	}
}

func TestManagerInternal_saveServerList(t *testing.T) {
	m := testManager()

	func() {
		l := m.getServerList()
		if len(l.servers) != 0 {
			t.Fatalf("Manager.saveServerList failed to load init config")
		}

		newServer := new(metadata.Server)
		l.servers = append(l.servers, newServer)
		m.saveServerList(l)
	}()

	func() {
		l1 := m.getServerList()
		t1NumServers := len(l1.servers)
		if t1NumServers != 1 {
			t.Fatalf("Manager.saveServerList failed to save mutated config")
		}
	}()

	func() {
		newServer := new(metadata.Server)
		l := m.getServerList()
		l.servers = append(l.servers, newServer)

		l_orig := m.getServerList()
		origNumServers := len(l_orig.servers)
		if origNumServers >= len(l.servers) {
			t.Fatalf("Manager.saveServerList unsaved config overwrote original")
		}
	}()
}
