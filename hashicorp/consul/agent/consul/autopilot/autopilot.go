package autopilot

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/as/consulrun/hashicorp/go-version"
	"github.com/as/consulrun/hashicorp/raft"
	"github.com/as/consulrun/hashicorp/serf/serf"
)

type Delegate interface {
	AutopilotConfig() *Config
	FetchStats(context.Context, []serf.Member) map[string]*ServerStats
	IsServer(serf.Member) (*ServerInfo, error)
	NotifyHealth(OperatorHealthReply)
	PromoteNonVoters(*Config, OperatorHealthReply) ([]raft.Server, error)
	Raft() *raft.Raft
	Serf() *serf.Serf
}

type Autopilot struct {
	logger   *log.Logger
	delegate Delegate

	interval       time.Duration
	healthInterval time.Duration

	clusterHealth     OperatorHealthReply
	clusterHealthLock sync.RWMutex

	enabled      bool
	removeDeadCh chan struct{}
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
	waitGroup    sync.WaitGroup
}

type ServerInfo struct {
	Name   string
	ID     string
	Addr   net.Addr
	Build  version.Version
	Status serf.MemberStatus
}

func NewAutopilot(logger *log.Logger, delegate Delegate, interval, healthInterval time.Duration) *Autopilot {
	return &Autopilot{
		logger:         logger,
		delegate:       delegate,
		interval:       interval,
		healthInterval: healthInterval,
		removeDeadCh:   make(chan struct{}),
	}
}

func (a *Autopilot) Start() {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.enabled {
		return
	}

	a.shutdownCh = make(chan struct{})
	a.waitGroup = sync.WaitGroup{}
	a.clusterHealth = OperatorHealthReply{}

	a.waitGroup.Add(2)
	go a.run()
	go a.serverHealthLoop()
	a.enabled = true
}

func (a *Autopilot) Stop() {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if !a.enabled {
		return
	}

	close(a.shutdownCh)
	a.waitGroup.Wait()
	a.enabled = false
}

func (a *Autopilot) run() {
	defer a.waitGroup.Done()

	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	for {
		select {
		case <-a.shutdownCh:
			return
		case <-ticker.C:
			if err := a.promoteServers(); err != nil {
				a.logger.Printf("[ERR] autopilot: Error promoting servers: %v", err)
			}

			if err := a.pruneDeadServers(); err != nil {
				a.logger.Printf("[ERR] autopilot: Error checking for dead servers to remove: %s", err)
			}
		case <-a.removeDeadCh:
			if err := a.pruneDeadServers(); err != nil {
				a.logger.Printf("[ERR] autopilot: Error checking for dead servers to remove: %s", err)
			}
		}
	}
}

func (a *Autopilot) promoteServers() error {
	conf := a.delegate.AutopilotConfig()
	if conf == nil {
		return nil
	}

	minRaftProtocol, err := a.MinRaftProtocol()
	if err != nil {
		return fmt.Errorf("error getting server raft protocol versions: %s", err)
	}
	if minRaftProtocol >= 3 {
		promotions, err := a.delegate.PromoteNonVoters(conf, a.GetClusterHealth())
		if err != nil {
			return fmt.Errorf("error checking for non-voters to promote: %s", err)
		}
		if err := a.handlePromotions(promotions); err != nil {
			return fmt.Errorf("error handling promotions: %s", err)
		}
	}

	return nil
}

func fmtServer(server raft.Server) string {
	return fmt.Sprintf("Server (ID: %q Address: %q)", server.ID, server.Address)
}

func NumPeers(raftConfig raft.Configuration) int {
	var numPeers int
	for _, server := range raftConfig.Servers {
		if server.Suffrage == raft.Voter {
			numPeers++
		}
	}
	return numPeers
}

func (a *Autopilot) RemoveDeadServers() {
	select {
	case a.removeDeadCh <- struct{}{}:
	default:
	}
}

func (a *Autopilot) pruneDeadServers() error {
	conf := a.delegate.AutopilotConfig()
	if conf == nil || !conf.CleanupDeadServers {
		return nil
	}

	var failed []string
	staleRaftServers := make(map[string]raft.Server)
	raftNode := a.delegate.Raft()
	future := raftNode.GetConfiguration()
	if err := future.Error(); err != nil {
		return err
	}

	raftConfig := future.Configuration()
	for _, server := range raftConfig.Servers {
		staleRaftServers[string(server.Address)] = server
	}

	serfLAN := a.delegate.Serf()
	for _, member := range serfLAN.Members() {
		server, err := a.delegate.IsServer(member)
		if err != nil {
			a.logger.Printf("[INFO] autopilot: Error parsing server info for %q: %s", member.Name, err)
			continue
		}
		if server != nil {

			if _, ok := staleRaftServers[server.Addr.String()]; ok {
				delete(staleRaftServers, server.Addr.String())
			}

			if member.Status == serf.StatusFailed {
				failed = append(failed, member.Name)
			}
		}
	}

	removalCount := len(failed) + len(staleRaftServers)
	if removalCount == 0 {
		return nil
	}

	peers := NumPeers(raftConfig)
	if removalCount < peers/2 {
		for _, node := range failed {
			a.logger.Printf("[INFO] autopilot: Attempting removal of failed server node %q", node)
			go serfLAN.RemoveFailedNode(node)
		}

		minRaftProtocol, err := a.MinRaftProtocol()
		if err != nil {
			return err
		}
		for _, raftServer := range staleRaftServers {
			a.logger.Printf("[INFO] autopilot: Attempting removal of stale %s", fmtServer(raftServer))
			var future raft.Future
			if minRaftProtocol >= 2 {
				future = raftNode.RemoveServer(raftServer.ID, 0, 0)
			} else {
				future = raftNode.RemovePeer(raftServer.Address)
			}
			if err := future.Error(); err != nil {
				return err
			}
		}
	} else {
		a.logger.Printf("[DEBUG] autopilot: Failed to remove dead servers: too many dead servers: %d/%d", removalCount, peers)
	}

	return nil
}

func (a *Autopilot) MinRaftProtocol() (int, error) {
	return minRaftProtocol(a.delegate.Serf().Members(), a.delegate.IsServer)
}

func minRaftProtocol(members []serf.Member, serverFunc func(serf.Member) (*ServerInfo, error)) (int, error) {
	minVersion := -1
	for _, m := range members {
		if m.Status != serf.StatusAlive {
			continue
		}

		server, err := serverFunc(m)
		if err != nil {
			return -1, err
		}
		if server == nil {
			continue
		}

		vsn, ok := m.Tags["raft_vsn"]
		if !ok {
			vsn = "1"
		}
		raftVsn, err := strconv.Atoi(vsn)
		if err != nil {
			return -1, err
		}

		if minVersion == -1 || raftVsn < minVersion {
			minVersion = raftVsn
		}
	}

	if minVersion == -1 {
		return minVersion, fmt.Errorf("No servers found")
	}

	return minVersion, nil
}

func (a *Autopilot) handlePromotions(promotions []raft.Server) error {

	for _, server := range promotions {
		a.logger.Printf("[INFO] autopilot: Promoting %s to voter", fmtServer(server))
		addFuture := a.delegate.Raft().AddVoter(server.ID, server.Address, 0, 0)
		if err := addFuture.Error(); err != nil {
			return fmt.Errorf("failed to add raft peer: %v", err)
		}
	}

	if len(promotions) > 0 {
		select {
		case a.removeDeadCh <- struct{}{}:
		default:
		}
	}
	return nil
}

func (a *Autopilot) serverHealthLoop() {
	defer a.waitGroup.Done()

	ticker := time.NewTicker(a.healthInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.shutdownCh:
			return
		case <-ticker.C:
			if err := a.updateClusterHealth(); err != nil {
				a.logger.Printf("[ERR] autopilot: Error updating cluster health: %s", err)
			}
		}
	}
}

func (a *Autopilot) updateClusterHealth() error {

	minRaftProtocol, err := a.MinRaftProtocol()
	if err != nil {
		return fmt.Errorf("error getting server raft protocol versions: %s", err)
	}
	if minRaftProtocol < 3 {
		return nil
	}

	autopilotConf := a.delegate.AutopilotConfig()

	if autopilotConf == nil {
		return nil
	}

	var serverMembers []serf.Member
	serverMap := make(map[string]*ServerInfo)
	for _, member := range a.delegate.Serf().Members() {
		if member.Status == serf.StatusLeft {
			continue
		}

		server, err := a.delegate.IsServer(member)
		if err != nil {
			a.logger.Printf("[INFO] autopilot: Error parsing server info for %q: %s", member.Name, err)
			continue
		}
		if server != nil {
			serverMap[server.ID] = server
			serverMembers = append(serverMembers, member)
		}
	}

	raftNode := a.delegate.Raft()
	future := raftNode.GetConfiguration()
	if err := future.Error(); err != nil {
		return fmt.Errorf("error getting Raft configuration %s", err)
	}
	servers := future.Configuration().Servers

	targetLastIndex := raftNode.LastIndex()
	var fetchList []*ServerInfo
	for _, server := range servers {
		if parts, ok := serverMap[string(server.ID)]; ok {
			fetchList = append(fetchList, parts)
		}
	}
	d := time.Now().Add(a.healthInterval / 2)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()
	fetchedStats := a.delegate.FetchStats(ctx, serverMembers)

	leader := raftNode.Leader()
	var clusterHealth OperatorHealthReply
	voterCount := 0
	healthyCount := 0
	healthyVoterCount := 0
	for _, server := range servers {
		health := ServerHealth{
			ID:          string(server.ID),
			Address:     string(server.Address),
			Leader:      server.Address == leader,
			LastContact: -1,
			Voter:       server.Suffrage == raft.Voter,
		}

		parts, ok := serverMap[string(server.ID)]
		if ok {
			health.Name = parts.Name
			health.SerfStatus = parts.Status
			health.Version = parts.Build.String()
			if stats, ok := fetchedStats[string(server.ID)]; ok {
				if err := a.updateServerHealth(&health, parts, stats, autopilotConf, targetLastIndex); err != nil {
					a.logger.Printf("[WARN] autopilot: Error updating server %s health: %s", fmtServer(server), err)
				}
			}
		} else {
			health.SerfStatus = serf.StatusNone
		}

		if health.Voter {
			voterCount++
		}
		if health.Healthy {
			healthyCount++
			if health.Voter {
				healthyVoterCount++
			}
		}

		clusterHealth.Servers = append(clusterHealth.Servers, health)
	}
	clusterHealth.Healthy = healthyCount == len(servers)

	requiredQuorum := voterCount/2 + 1
	if healthyVoterCount > requiredQuorum {
		clusterHealth.FailureTolerance = healthyVoterCount - requiredQuorum
	}

	a.delegate.NotifyHealth(clusterHealth)

	a.clusterHealthLock.Lock()
	a.clusterHealth = clusterHealth
	a.clusterHealthLock.Unlock()

	return nil
}

func (a *Autopilot) updateServerHealth(health *ServerHealth,
	server *ServerInfo, stats *ServerStats,
	autopilotConf *Config, targetLastIndex uint64) error {

	health.LastTerm = stats.LastTerm
	health.LastIndex = stats.LastIndex

	if stats.LastContact != "never" {
		var err error
		health.LastContact, err = time.ParseDuration(stats.LastContact)
		if err != nil {
			return fmt.Errorf("error parsing last_contact duration: %s", err)
		}
	}

	raftNode := a.delegate.Raft()
	lastTerm, err := strconv.ParseUint(raftNode.Stats()["last_log_term"], 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing last_log_term: %s", err)
	}
	health.Healthy = health.IsHealthy(lastTerm, targetLastIndex, autopilotConf)

	lastHealth := a.GetServerHealth(server.ID)
	if lastHealth == nil || lastHealth.Healthy != health.Healthy {
		health.StableSince = time.Now()
	} else {
		health.StableSince = lastHealth.StableSince
	}

	return nil
}

func (a *Autopilot) GetClusterHealth() OperatorHealthReply {
	a.clusterHealthLock.RLock()
	defer a.clusterHealthLock.RUnlock()
	return a.clusterHealth
}

func (a *Autopilot) GetServerHealth(id string) *ServerHealth {
	a.clusterHealthLock.RLock()
	defer a.clusterHealthLock.RUnlock()
	return a.clusterHealth.ServerHealth(id)
}

func IsPotentialVoter(suffrage raft.ServerSuffrage) bool {
	switch suffrage {
	case raft.Voter, raft.Staging:
		return true
	default:
		return false
	}
}
