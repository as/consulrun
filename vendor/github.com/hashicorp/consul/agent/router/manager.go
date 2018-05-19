// Package servers provides a Manager interface for Manager managed
package router

import (
	"log"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/agent/metadata"
	"github.com/hashicorp/consul/lib"
)

const (

	//

	clientRPCJitterFraction = 2

	clientRPCMinReuseDuration = 120 * time.Second

	//

	newRebalanceConnsPerSecPerServer = 64
)

type ManagerSerfCluster interface {
	NumNodes() int
}

type Pinger interface {
	Ping(dc string, addr net.Addr, version int, useTLS bool) (bool, error)
}

//
// be copied onto the stack.  Please keep this structure light.
type serverList struct {
	servers []*metadata.Server
}

type Manager struct {
	listValue atomic.Value
	listLock  sync.Mutex

	rebalanceTimer *time.Timer

	shutdownCh chan struct{}

	logger *log.Logger

	clusterInfo ManagerSerfCluster

	connPoolPinger Pinger

	notifyFailedBarrier int32

	offline int32
}

func (m *Manager) AddServer(s *metadata.Server) {
	m.listLock.Lock()
	defer m.listLock.Unlock()
	l := m.getServerList()

	found := false
	for idx, existing := range l.servers {
		if existing.Name == s.Name {
			newServers := make([]*metadata.Server, len(l.servers))
			copy(newServers, l.servers)

			newServers[idx] = s

			l.servers = newServers
			found = true
			break
		}
	}

	if !found {
		newServers := make([]*metadata.Server, len(l.servers), len(l.servers)+1)
		copy(newServers, l.servers)
		newServers = append(newServers, s)
		l.servers = newServers
	}

	atomic.StoreInt32(&m.offline, 0)

	m.saveServerList(l)
}

func (l *serverList) cycleServer() (servers []*metadata.Server) {
	numServers := len(l.servers)
	if numServers < 2 {
		return servers // No action required
	}

	newServers := make([]*metadata.Server, 0, numServers)
	newServers = append(newServers, l.servers[1:]...)
	newServers = append(newServers, l.servers[0])

	return newServers
}

func (l *serverList) removeServerByKey(targetKey *metadata.Key) {
	for i, s := range l.servers {
		if targetKey.Equal(s.Key()) {
			copy(l.servers[i:], l.servers[i+1:])
			l.servers[len(l.servers)-1] = nil
			l.servers = l.servers[:len(l.servers)-1]
			return
		}
	}
}

func (l *serverList) shuffleServers() {
	for i := len(l.servers) - 1; i > 0; i-- {
		j := rand.Int31n(int32(i + 1))
		l.servers[i], l.servers[j] = l.servers[j], l.servers[i]
	}
}

func (m *Manager) IsOffline() bool {
	offline := atomic.LoadInt32(&m.offline)
	return offline == 1
}

func (m *Manager) FindServer() *metadata.Server {
	l := m.getServerList()
	numServers := len(l.servers)
	if numServers == 0 {
		m.logger.Printf("[WARN] manager: No servers available")
		return nil
	}

	return l.servers[0]
}

func (m *Manager) getServerList() serverList {
	return m.listValue.Load().(serverList)
}

func (m *Manager) saveServerList(l serverList) {
	m.listValue.Store(l)
}

func New(logger *log.Logger, shutdownCh chan struct{}, clusterInfo ManagerSerfCluster, connPoolPinger Pinger) (m *Manager) {
	m = new(Manager)
	m.logger = logger
	m.clusterInfo = clusterInfo       // can't pass *consul.Client: import cycle
	m.connPoolPinger = connPoolPinger // can't pass *consul.ConnPool: import cycle
	m.rebalanceTimer = time.NewTimer(clientRPCMinReuseDuration)
	m.shutdownCh = shutdownCh
	atomic.StoreInt32(&m.offline, 1)

	l := serverList{}
	l.servers = make([]*metadata.Server, 0)
	m.saveServerList(l)
	return m
}

func (m *Manager) NotifyFailedServer(s *metadata.Server) {
	l := m.getServerList()

	if len(l.servers) > 1 && l.servers[0].Name == s.Name &&

		atomic.CompareAndSwapInt32(&m.notifyFailedBarrier, 0, 1) {
		defer atomic.StoreInt32(&m.notifyFailedBarrier, 0)

		m.listLock.Lock()
		defer m.listLock.Unlock()
		l = m.getServerList()

		if len(l.servers) > 1 && l.servers[0].Name == s.Name {
			l.servers = l.cycleServer()
			m.saveServerList(l)
			m.logger.Printf(`[DEBUG] manager: cycled away from server "%s"`, s.Name)
		}
	}
}

func (m *Manager) NumServers() int {
	l := m.getServerList()
	return len(l.servers)
}

//
func (m *Manager) RebalanceServers() {

	l := m.getServerList()

	l.shuffleServers()

	var foundHealthyServer bool
	for i := 0; i < len(l.servers); i++ {

		srv := l.servers[0]

		ok, err := m.connPoolPinger.Ping(srv.Datacenter, srv.Addr, srv.Version, srv.UseTLS)
		if ok {
			foundHealthyServer = true
			break
		}
		m.logger.Printf(`[DEBUG] manager: pinging server "%s" failed: %s`, srv, err)
		l.servers = l.cycleServer()
	}

	if foundHealthyServer {
		atomic.StoreInt32(&m.offline, 0)
	} else {
		atomic.StoreInt32(&m.offline, 1)
		m.logger.Printf("[DEBUG] manager: No healthy servers during rebalance, aborting")
		return
	}

	if m.reconcileServerList(&l) {
		m.logger.Printf("[DEBUG] manager: Rebalanced %d servers, next active server is %s", len(l.servers), l.servers[0].String())
	} else {

		//

	}

	return
}

func (m *Manager) reconcileServerList(l *serverList) bool {
	m.listLock.Lock()
	defer m.listLock.Unlock()

	newServerCfg := m.getServerList()

	if len(newServerCfg.servers) == 0 || len(l.servers) == 0 {
		return false
	}

	type targetServer struct {
		server *metadata.Server

		state byte
	}
	mergedList := make(map[metadata.Key]*targetServer, len(l.servers))
	for _, s := range l.servers {
		mergedList[*s.Key()] = &targetServer{server: s, state: 'o'}
	}
	for _, s := range newServerCfg.servers {
		k := s.Key()
		_, found := mergedList[*k]
		if found {
			mergedList[*k].state = 'b'
		} else {
			mergedList[*k] = &targetServer{server: s, state: 'n'}
		}
	}

	selectedServerKey := l.servers[0].Key()
	if v, found := mergedList[*selectedServerKey]; found && v.state == 'o' {
		return false
	}

	for k, v := range mergedList {
		switch v.state {
		case 'b':

		case 'o':

			l.removeServerByKey(&k)
		case 'n':

			l.servers = append(l.servers, v.server)
		default:
			panic("unknown merge list state")
		}
	}

	m.saveServerList(*l)
	return true
}

func (m *Manager) RemoveServer(s *metadata.Server) {
	m.listLock.Lock()
	defer m.listLock.Unlock()
	l := m.getServerList()

	for i := range l.servers {
		if l.servers[i].Name == s.Name {
			newServers := make([]*metadata.Server, 0, len(l.servers)-1)
			newServers = append(newServers, l.servers[:i]...)
			newServers = append(newServers, l.servers[i+1:]...)
			l.servers = newServers

			m.saveServerList(l)
			return
		}
	}
}

func (m *Manager) refreshServerRebalanceTimer() time.Duration {
	l := m.getServerList()
	numServers := len(l.servers)

	clusterWideRebalanceConnsPerSec := float64(numServers * newRebalanceConnsPerSecPerServer)
	connReuseLowWatermarkDuration := clientRPCMinReuseDuration + lib.RandomStagger(clientRPCMinReuseDuration/clientRPCJitterFraction)
	numLANMembers := m.clusterInfo.NumNodes()
	connRebalanceTimeout := lib.RateScaledInterval(clusterWideRebalanceConnsPerSec, connReuseLowWatermarkDuration, numLANMembers)

	m.rebalanceTimer.Reset(connRebalanceTimeout)
	return connRebalanceTimeout
}

func (m *Manager) ResetRebalanceTimer() {
	m.listLock.Lock()
	defer m.listLock.Unlock()
	m.rebalanceTimer.Reset(clientRPCMinReuseDuration)
}

func (m *Manager) Start() {
	for {
		select {
		case <-m.rebalanceTimer.C:
			m.RebalanceServers()
			m.refreshServerRebalanceTimer()

		case <-m.shutdownCh:
			m.logger.Printf("[INFO] manager: shutting down")
			return
		}
	}
}
