package router_test

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/as/consulrun/hashicorp/consul/agent/metadata"
	"github.com/as/consulrun/hashicorp/consul/agent/router"
)

type fauxAddr struct {
	addr string
}

func (a *fauxAddr) Network() string {
	return "faux"
}

func (a *fauxAddr) String() string {
	return a.addr
}

type fauxConnPool struct {
	failPct float64

	failAddr net.Addr
}

func (cp *fauxConnPool) Ping(dc string, addr net.Addr, version int, useTLS bool) (bool, error) {
	var success bool

	successProb := rand.Float64()
	if successProb > cp.failPct {
		success = true
	}

	if cp.failAddr != nil && addr.String() == cp.failAddr.String() {
		success = false
	}

	return success, nil
}

type fauxSerf struct {
}

func (s *fauxSerf) NumNodes() int {
	return 16384
}

func testManager() (m *router.Manager) {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	shutdownCh := make(chan struct{})
	m = router.New(logger, shutdownCh, &fauxSerf{}, &fauxConnPool{})
	return m
}

func testManagerFailProb(failPct float64) (m *router.Manager) {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	shutdownCh := make(chan struct{})
	m = router.New(logger, shutdownCh, &fauxSerf{}, &fauxConnPool{failPct: failPct})
	return m
}

func testManagerFailAddr(failAddr net.Addr) (m *router.Manager) {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	shutdownCh := make(chan struct{})
	m = router.New(logger, shutdownCh, &fauxSerf{}, &fauxConnPool{failAddr: failAddr})
	return m
}

func TestServers_AddServer(t *testing.T) {
	m := testManager()
	var num int
	num = m.NumServers()
	if num != 0 {
		t.Fatalf("Expected zero servers to start")
	}

	s1 := &metadata.Server{Name: "s1"}
	m.AddServer(s1)
	num = m.NumServers()
	if num != 1 {
		t.Fatalf("Expected one server")
	}

	m.AddServer(s1)
	num = m.NumServers()
	if num != 1 {
		t.Fatalf("Expected one server (still)")
	}

	s2 := &metadata.Server{Name: "s2"}
	m.AddServer(s2)
	num = m.NumServers()
	if num != 2 {
		t.Fatalf("Expected two servers")
	}
}

func TestServers_IsOffline(t *testing.T) {
	m := testManager()
	if !m.IsOffline() {
		t.Fatalf("bad")
	}

	s1 := &metadata.Server{Name: "s1"}
	m.AddServer(s1)
	if m.IsOffline() {
		t.Fatalf("bad")
	}
	m.RebalanceServers()
	if m.IsOffline() {
		t.Fatalf("bad")
	}
	m.RemoveServer(s1)
	m.RebalanceServers()
	if !m.IsOffline() {
		t.Fatalf("bad")
	}

	const failPct = 0.5
	m = testManagerFailProb(failPct)
	m.AddServer(s1)
	var on, off int
	for i := 0; i < 100; i++ {
		m.RebalanceServers()
		if m.IsOffline() {
			off++
		} else {
			on++
		}
	}
	if on == 0 || off == 0 {
		t.Fatalf("bad: %d %d", on, off)
	}
}

func TestServers_FindServer(t *testing.T) {
	m := testManager()

	if m.FindServer() != nil {
		t.Fatalf("Expected nil return")
	}

	m.AddServer(&metadata.Server{Name: "s1"})
	if m.NumServers() != 1 {
		t.Fatalf("Expected one server")
	}

	s1 := m.FindServer()
	if s1 == nil {
		t.Fatalf("Expected non-nil server")
	}
	if s1.Name != "s1" {
		t.Fatalf("Expected s1 server")
	}

	s1 = m.FindServer()
	if s1 == nil || s1.Name != "s1" {
		t.Fatalf("Expected s1 server (still)")
	}

	m.AddServer(&metadata.Server{Name: "s2"})
	if m.NumServers() != 2 {
		t.Fatalf("Expected two servers")
	}
	s1 = m.FindServer()
	if s1 == nil || s1.Name != "s1" {
		t.Fatalf("Expected s1 server (still)")
	}

	m.NotifyFailedServer(s1)
	s2 := m.FindServer()
	if s2 == nil || s2.Name != "s2" {
		t.Fatalf("Expected s2 server")
	}

	m.NotifyFailedServer(s2)
	s1 = m.FindServer()
	if s1 == nil || s1.Name != "s1" {
		t.Fatalf("Expected s1 server")
	}
}

func TestServers_New(t *testing.T) {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	shutdownCh := make(chan struct{})
	m := router.New(logger, shutdownCh, &fauxSerf{}, &fauxConnPool{})
	if m == nil {
		t.Fatalf("Manager nil")
	}
}

func TestServers_NotifyFailedServer(t *testing.T) {
	m := testManager()

	if m.NumServers() != 0 {
		t.Fatalf("Expected zero servers to start")
	}

	s1 := &metadata.Server{Name: "s1"}
	s2 := &metadata.Server{Name: "s2"}

	m.NotifyFailedServer(s1)
	if m.NumServers() != 0 {
		t.Fatalf("Expected zero servers to start")
	}
	m.AddServer(s1)

	m.NotifyFailedServer(s2)
	if m.NumServers() != 1 {
		t.Fatalf("Expected one server")
	}

	m.AddServer(s2)
	if m.NumServers() != 2 {
		t.Fatalf("Expected two servers")
	}

	s1 = m.FindServer()
	if s1 == nil || s1.Name != "s1" {
		t.Fatalf("Expected s1 server")
	}

	m.NotifyFailedServer(s2)
	s1 = m.FindServer()
	if s1 == nil || s1.Name != "s1" {
		t.Fatalf("Expected s1 server (still)")
	}

	m.NotifyFailedServer(s1)
	s2 = m.FindServer()
	if s2 == nil || s2.Name != "s2" {
		t.Fatalf("Expected s2 server")
	}

	m.NotifyFailedServer(s2)
	s1 = m.FindServer()
	if s1 == nil || s1.Name != "s1" {
		t.Fatalf("Expected s1 server")
	}
}

func TestServers_NumServers(t *testing.T) {
	m := testManager()
	var num int
	num = m.NumServers()
	if num != 0 {
		t.Fatalf("Expected zero servers to start")
	}

	s := &metadata.Server{}
	m.AddServer(s)
	num = m.NumServers()
	if num != 1 {
		t.Fatalf("Expected one server after AddServer")
	}
}

func TestServers_RebalanceServers(t *testing.T) {
	const failPct = 0.5
	m := testManagerFailProb(failPct)
	const maxServers = 100
	const numShuffleTests = 100
	const uniquePassRate = 0.5

	for i := 0; i < maxServers; i++ {
		nodeName := fmt.Sprintf("s%02d", i)
		m.AddServer(&metadata.Server{Name: nodeName})
	}

	uniques := make(map[string]struct{}, maxServers)
	for i := 0; i < numShuffleTests; i++ {
		m.RebalanceServers()

		var names []string
		for j := 0; j < maxServers; j++ {
			server := m.FindServer()
			m.NotifyFailedServer(server)
			names = append(names, server.Name)
		}
		key := strings.Join(names, "|")
		uniques[key] = struct{}{}
	}

	if len(uniques) < int(maxServers*uniquePassRate) {
		t.Fatalf("unique shuffle ratio too low: %d/%d", len(uniques), maxServers)
	}
}

func TestServers_RebalanceServers_AvoidFailed(t *testing.T) {

	servers := []*metadata.Server{
		{Name: "s1", Addr: &fauxAddr{"s1"}},
		{Name: "s2", Addr: &fauxAddr{"s2"}},
		{Name: "s3", Addr: &fauxAddr{"s3"}},
	}
	for i := 0; i < 100; i++ {
		m := testManagerFailAddr(&fauxAddr{"s2"})
		for _, s := range servers {
			m.AddServer(s)
		}

		m.RebalanceServers()
		if front := m.FindServer().Name; front == "s2" {
			t.Fatalf("should have avoided the failed server")
		}
	}
}

func TestManager_RemoveServer(t *testing.T) {
	const nodeNameFmt = "s%02d"
	m := testManager()

	if m.NumServers() != 0 {
		t.Fatalf("Expected zero servers to start")
	}

	nodeName := fmt.Sprintf(nodeNameFmt, 1)
	s1 := &metadata.Server{Name: nodeName}
	m.RemoveServer(s1)
	m.AddServer(s1)

	nodeName = fmt.Sprintf(nodeNameFmt, 2)
	s2 := &metadata.Server{Name: nodeName}
	m.RemoveServer(s2)
	m.AddServer(s2)

	const maxServers = 19
	servers := make([]*metadata.Server, maxServers)

	for i := maxServers; i > 2; i-- {
		nodeName := fmt.Sprintf(nodeNameFmt, i)
		server := &metadata.Server{Name: nodeName}
		servers = append(servers, server)
		m.AddServer(server)
	}
	if m.NumServers() != maxServers {
		t.Fatalf("Expected %d servers, received %d", maxServers, m.NumServers())
	}

	m.RebalanceServers()

	if m.NumServers() != maxServers {
		t.Fatalf("Expected %d servers, received %d", maxServers, m.NumServers())
	}

	findServer := func(server *metadata.Server) bool {
		for i := m.NumServers(); i > 0; i-- {
			s := m.FindServer()
			if s == server {
				return true
			}
		}
		return false
	}

	expectedNumServers := maxServers
	removedServers := make([]*metadata.Server, 0, maxServers)

	for i := 3; i > 0; i-- {
		server := m.FindServer()
		if server == nil {
			t.Fatalf("FindServer returned nil")
		}
		m.RemoveServer(server)
		expectedNumServers--
		if m.NumServers() != expectedNumServers {
			t.Fatalf("Expected %d servers (got %d)", expectedNumServers, m.NumServers())
		}
		if findServer(server) == true {
			t.Fatalf("Did not expect to find server %s after removal from the front", server.Name)
		}
		removedServers = append(removedServers, server)
	}

	for i := 3; i > 0; i-- {
		server := m.FindServer()
		m.NotifyFailedServer(server)
		m.RemoveServer(server)
		expectedNumServers--
		if m.NumServers() != expectedNumServers {
			t.Fatalf("Expected %d servers (got %d)", expectedNumServers, m.NumServers())
		}
		if findServer(server) == true {
			t.Fatalf("Did not expect to find server %s", server.Name)
		}
		removedServers = append(removedServers, server)
	}

	for i := 3; i > 0; i-- {
		server := m.FindServer()
		m.NotifyFailedServer(server)
		server2 := m.FindServer()
		m.NotifyFailedServer(server2) // server2 now at end of the list

		m.RemoveServer(server)
		expectedNumServers--
		if m.NumServers() != expectedNumServers {
			t.Fatalf("Expected %d servers (got %d)", expectedNumServers, m.NumServers())
		}
		if findServer(server) == true {
			t.Fatalf("Did not expect to find server %s", server.Name)
		}
		removedServers = append(removedServers, server)
	}

	if m.NumServers()+len(removedServers) != maxServers {
		t.Fatalf("Expected %d+%d=%d servers", m.NumServers(), len(removedServers), maxServers)
	}

	for i := m.NumServers(); i > 0; i-- {
		server := m.FindServer()
		m.NotifyFailedServer(server)
		server2 := m.FindServer()
		m.NotifyFailedServer(server2) // server2 now at end of the list
		m.RemoveServer(server)
		removedServers = append(removedServers, server)
	}

	if m.NumServers() != 0 {
		t.Fatalf("Expected an empty server list")
	}
	if len(removedServers) != maxServers {
		t.Fatalf("Expected all servers to be in removed server list")
	}
}
