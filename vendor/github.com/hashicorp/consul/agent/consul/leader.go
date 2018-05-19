package consul

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/consul/autopilot"
	"github.com/hashicorp/consul/agent/metadata"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/types"
	"github.com/hashicorp/go-version"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

const (
	newLeaderEvent      = "consul:new-leader"
	barrierWriteTimeout = 2 * time.Minute
)

var minAutopilotVersion = version.Must(version.NewVersion("0.8.0"))

func (s *Server) monitorLeadership() {

	raftNotifyCh := s.raftNotifyCh

	var weAreLeaderCh chan struct{}
	var leaderLoop sync.WaitGroup
	for {
		select {
		case isLeader := <-raftNotifyCh:
			switch {
			case isLeader:
				if weAreLeaderCh != nil {
					s.logger.Printf("[ERR] consul: attempted to start the leader loop while running")
					continue
				}

				weAreLeaderCh = make(chan struct{})
				leaderLoop.Add(1)
				go func(ch chan struct{}) {
					defer leaderLoop.Done()
					s.leaderLoop(ch)
				}(weAreLeaderCh)
				s.logger.Printf("[INFO] consul: cluster leadership acquired")

			default:
				if weAreLeaderCh == nil {
					s.logger.Printf("[ERR] consul: attempted to stop the leader loop while not running")
					continue
				}

				s.logger.Printf("[DEBUG] consul: shutting down leader loop")
				close(weAreLeaderCh)
				leaderLoop.Wait()
				weAreLeaderCh = nil
				s.logger.Printf("[INFO] consul: cluster leadership lost")
			}

		case <-s.shutdownCh:
			return
		}
	}
}

func (s *Server) leaderLoop(stopCh chan struct{}) {

	payload := []byte(s.config.NodeName)
	for name, segment := range s.LANSegments() {
		if err := segment.UserEvent(newLeaderEvent, payload, false); err != nil {
			s.logger.Printf("[WARN] consul: failed to broadcast new leader event on segment %q: %v", name, err)
		}
	}

	var reconcileCh chan serf.Member
	establishedLeader := false

	reassert := func() error {
		if !establishedLeader {
			return fmt.Errorf("leadership has not been established")
		}
		if err := s.revokeLeadership(); err != nil {
			return err
		}
		if err := s.establishLeadership(); err != nil {
			return err
		}
		return nil
	}

RECONCILE:

	reconcileCh = nil
	interval := time.After(s.config.ReconcileInterval)

	start := time.Now()
	barrier := s.raft.Barrier(barrierWriteTimeout)
	if err := barrier.Error(); err != nil {
		s.logger.Printf("[ERR] consul: failed to wait for barrier: %v", err)
		goto WAIT
	}
	metrics.MeasureSince([]string{"consul", "leader", "barrier"}, start)
	metrics.MeasureSince([]string{"leader", "barrier"}, start)

	if !establishedLeader {
		if err := s.establishLeadership(); err != nil {
			s.logger.Printf("[ERR] consul: failed to establish leadership: %v", err)

			if err := s.revokeLeadership(); err != nil {
				s.logger.Printf("[ERR] consul: failed to revoke leadership: %v", err)
			}
			goto WAIT
		}
		establishedLeader = true
		defer func() {
			if err := s.revokeLeadership(); err != nil {
				s.logger.Printf("[ERR] consul: failed to revoke leadership: %v", err)
			}
		}()
	}

	if err := s.reconcile(); err != nil {
		s.logger.Printf("[ERR] consul: failed to reconcile: %v", err)
		goto WAIT
	}

	reconcileCh = s.reconcileCh

WAIT:

	select {
	case <-stopCh:
		return
	default:
	}

	for {
		select {
		case <-stopCh:
			return
		case <-s.shutdownCh:
			return
		case <-interval:
			goto RECONCILE
		case member := <-reconcileCh:
			s.reconcileMember(member)
		case index := <-s.tombstoneGC.ExpireCh():
			go s.reapTombstones(index)
		case errCh := <-s.reassertLeaderCh:
			errCh <- reassert()
		}
	}
}

func (s *Server) establishLeadership() error {
	defer metrics.MeasureSince([]string{"consul", "leader", "establish_leadership"}, time.Now())

	if err := s.initializeACL(); err != nil {
		return err
	}

	s.tombstoneGC.SetEnabled(true)
	lastIndex := s.raft.LastIndex()
	s.tombstoneGC.Hint(lastIndex)

	//

	if err := s.initializeSessionTimers(); err != nil {
		return err
	}

	s.getOrCreateAutopilotConfig()
	s.autopilot.Start()
	s.setConsistentReadReady()
	return nil
}

func (s *Server) revokeLeadership() error {
	defer metrics.MeasureSince([]string{"consul", "leader", "revoke_leadership"}, time.Now())

	s.tombstoneGC.SetEnabled(false)

	if err := s.clearAllSessionTimers(); err != nil {
		return err
	}

	s.resetConsistentReadReady()
	s.autopilot.Stop()
	return nil
}

func (s *Server) initializeACL() error {

	authDC := s.config.ACLDatacenter
	if len(authDC) == 0 || authDC != s.config.Datacenter {
		return nil
	}

	s.aclAuthCache.Purge()

	state := s.fsm.State()
	_, acl, err := state.ACLGet(nil, anonymousToken)
	if err != nil {
		return fmt.Errorf("failed to get anonymous token: %v", err)
	}
	if acl == nil {
		req := structs.ACLRequest{
			Datacenter: authDC,
			Op:         structs.ACLSet,
			ACL: structs.ACL{
				ID:   anonymousToken,
				Name: "Anonymous Token",
				Type: structs.ACLTypeClient,
			},
		}
		_, err := s.raftApply(structs.ACLRequestType, &req)
		if err != nil {
			return fmt.Errorf("failed to create anonymous token: %v", err)
		}
	}

	if master := s.config.ACLMasterToken; len(master) > 0 {
		_, acl, err = state.ACLGet(nil, master)
		if err != nil {
			return fmt.Errorf("failed to get master token: %v", err)
		}
		if acl == nil {
			req := structs.ACLRequest{
				Datacenter: authDC,
				Op:         structs.ACLSet,
				ACL: structs.ACL{
					ID:   master,
					Name: "Master Token",
					Type: structs.ACLTypeManagement,
				},
			}
			_, err := s.raftApply(structs.ACLRequestType, &req)
			if err != nil {
				return fmt.Errorf("failed to create master token: %v", err)
			}
			s.logger.Printf("[INFO] consul: Created ACL master token from configuration")
		}
	}

	var minVersion = version.Must(version.NewVersion("0.9.1"))
	if ServersMeetMinimumVersion(s.LANMembers(), minVersion) {
		bs, err := state.ACLGetBootstrap()
		if err != nil {
			return fmt.Errorf("failed looking for ACL bootstrap info: %v", err)
		}
		if bs == nil {
			req := structs.ACLRequest{
				Datacenter: authDC,
				Op:         structs.ACLBootstrapInit,
			}
			resp, err := s.raftApply(structs.ACLRequestType, &req)
			if err != nil {
				return fmt.Errorf("failed to initialize ACL bootstrap: %v", err)
			}
			switch v := resp.(type) {
			case error:
				return fmt.Errorf("failed to initialize ACL bootstrap: %v", v)

			case bool:
				if v {
					s.logger.Printf("[INFO] consul: ACL bootstrap enabled")
				} else {
					s.logger.Printf("[INFO] consul: ACL bootstrap disabled, existing management tokens found")
				}

			default:
				return fmt.Errorf("unexpected response trying to initialize ACL bootstrap: %T", v)
			}
		}
	} else {
		s.logger.Printf("[WARN] consul: Can't initialize ACL bootstrap until all servers are >= %s", minVersion.String())
	}

	return nil
}

func (s *Server) getOrCreateAutopilotConfig() *autopilot.Config {
	state := s.fsm.State()
	_, config, err := state.AutopilotConfig()
	if err != nil {
		s.logger.Printf("[ERR] autopilot: failed to get config: %v", err)
		return nil
	}
	if config != nil {
		return config
	}

	if !ServersMeetMinimumVersion(s.LANMembers(), minAutopilotVersion) {
		s.logger.Printf("[WARN] autopilot: can't initialize until all servers are >= %s", minAutopilotVersion.String())
		return nil
	}

	config = s.config.AutopilotConfig
	req := structs.AutopilotSetConfigRequest{Config: *config}
	if _, err = s.raftApply(structs.AutopilotRequestType, req); err != nil {
		s.logger.Printf("[ERR] autopilot: failed to initialize config: %v", err)
		return nil
	}

	return config
}

func (s *Server) reconcileReaped(known map[string]struct{}) error {
	state := s.fsm.State()
	_, checks, err := state.ChecksInState(nil, api.HealthAny)
	if err != nil {
		return err
	}
	for _, check := range checks {

		if check.CheckID != structs.SerfCheckID {
			continue
		}

		if _, ok := known[check.Node]; ok {
			continue
		}

		_, services, err := state.NodeServices(nil, check.Node)
		if err != nil {
			return err
		}
		serverPort := 0
		serverAddr := ""
		serverID := ""

	CHECKS:
		for _, service := range services.Services {
			if service.ID == structs.ConsulServiceID {
				_, node, err := state.GetNode(check.Node)
				if err != nil {
					s.logger.Printf("[ERR] consul: Unable to look up node with name %q: %v", check.Node, err)
					continue CHECKS
				}

				serverAddr = node.Address
				serverPort = service.Port
				lookupAddr := net.JoinHostPort(serverAddr, strconv.Itoa(serverPort))
				svr := s.serverLookup.Server(raft.ServerAddress(lookupAddr))
				if svr != nil {
					serverID = svr.ID
				}
				break
			}
		}

		member := serf.Member{
			Name: check.Node,
			Tags: map[string]string{
				"dc":   s.config.Datacenter,
				"role": "node",
			},
		}

		if serverPort > 0 {
			member.Tags["role"] = "consul"
			member.Tags["port"] = strconv.FormatUint(uint64(serverPort), 10)
			member.Tags["id"] = serverID
			member.Addr = net.ParseIP(serverAddr)
		}

		if err := s.handleReapMember(member); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) reconcileMember(member serf.Member) error {

	if !s.shouldHandleMember(member) {
		s.logger.Printf("[WARN] consul: skipping reconcile of node %v", member)
		return nil
	}
	defer metrics.MeasureSince([]string{"consul", "leader", "reconcileMember"}, time.Now())
	defer metrics.MeasureSince([]string{"leader", "reconcileMember"}, time.Now())
	var err error
	switch member.Status {
	case serf.StatusAlive:
		err = s.handleAliveMember(member)
	case serf.StatusFailed:
		err = s.handleFailedMember(member)
	case serf.StatusLeft:
		err = s.handleLeftMember(member)
	case StatusReap:
		err = s.handleReapMember(member)
	}
	if err != nil {
		s.logger.Printf("[ERR] consul: failed to reconcile member: %v: %v",
			member, err)

		if acl.IsErrPermissionDenied(err) {
			return nil
		}
	}
	return nil
}

func (s *Server) shouldHandleMember(member serf.Member) bool {
	if valid, dc := isConsulNode(member); valid && dc == s.config.Datacenter {
		return true
	}
	if valid, parts := metadata.IsConsulServer(member); valid &&
		parts.Segment == "" &&
		parts.Datacenter == s.config.Datacenter {
		return true
	}
	return false
}

func (s *Server) handleAliveMember(member serf.Member) error {

	var service *structs.NodeService
	if valid, parts := metadata.IsConsulServer(member); valid {
		service = &structs.NodeService{
			ID:      structs.ConsulServiceID,
			Service: structs.ConsulServiceName,
			Port:    parts.Port,
		}

		if err := s.joinConsulServer(member, parts); err != nil {
			return err
		}
	}

	state := s.fsm.State()
	_, node, err := state.GetNode(member.Name)
	if err != nil {
		return err
	}
	if node != nil && node.Address == member.Addr.String() {

		if service != nil {
			match := false
			_, services, err := state.NodeServices(nil, member.Name)
			if err != nil {
				return err
			}
			if services != nil {
				for id := range services.Services {
					if id == service.ID {
						match = true
					}
				}
			}
			if !match {
				goto AFTER_CHECK
			}
		}

		_, checks, err := state.NodeChecks(nil, member.Name)
		if err != nil {
			return err
		}
		for _, check := range checks {
			if check.CheckID == structs.SerfCheckID && check.Status == api.HealthPassing {
				return nil
			}
		}
	}
AFTER_CHECK:
	s.logger.Printf("[INFO] consul: member '%s' joined, marking health alive", member.Name)

	req := structs.RegisterRequest{
		Datacenter: s.config.Datacenter,
		Node:       member.Name,
		ID:         types.NodeID(member.Tags["id"]),
		Address:    member.Addr.String(),
		Service:    service,
		Check: &structs.HealthCheck{
			Node:    member.Name,
			CheckID: structs.SerfCheckID,
			Name:    structs.SerfCheckName,
			Status:  api.HealthPassing,
			Output:  structs.SerfCheckAliveOutput,
		},

		SkipNodeUpdate: true,
	}
	_, err = s.raftApply(structs.RegisterRequestType, &req)
	return err
}

func (s *Server) handleFailedMember(member serf.Member) error {

	state := s.fsm.State()
	_, node, err := state.GetNode(member.Name)
	if err != nil {
		return err
	}
	if node != nil && node.Address == member.Addr.String() {

		_, checks, err := state.NodeChecks(nil, member.Name)
		if err != nil {
			return err
		}
		for _, check := range checks {
			if check.CheckID == structs.SerfCheckID && check.Status == api.HealthCritical {
				return nil
			}
		}
	}
	s.logger.Printf("[INFO] consul: member '%s' failed, marking health critical", member.Name)

	req := structs.RegisterRequest{
		Datacenter: s.config.Datacenter,
		Node:       member.Name,
		ID:         types.NodeID(member.Tags["id"]),
		Address:    member.Addr.String(),
		Check: &structs.HealthCheck{
			Node:    member.Name,
			CheckID: structs.SerfCheckID,
			Name:    structs.SerfCheckName,
			Status:  api.HealthCritical,
			Output:  structs.SerfCheckFailedOutput,
		},

		SkipNodeUpdate: true,
	}
	_, err = s.raftApply(structs.RegisterRequestType, &req)
	return err
}

func (s *Server) handleLeftMember(member serf.Member) error {
	return s.handleDeregisterMember("left", member)
}

func (s *Server) handleReapMember(member serf.Member) error {
	return s.handleDeregisterMember("reaped", member)
}

func (s *Server) handleDeregisterMember(reason string, member serf.Member) error {

	if member.Name == s.config.NodeName {
		s.logger.Printf("[WARN] consul: deregistering self (%s) should be done by follower", s.config.NodeName)
		return nil
	}

	if valid, parts := metadata.IsConsulServer(member); valid {
		if err := s.removeConsulServer(member, parts.Port); err != nil {
			return err
		}
	}

	state := s.fsm.State()
	_, node, err := state.GetNode(member.Name)
	if err != nil {
		return err
	}
	if node == nil {
		return nil
	}

	s.logger.Printf("[INFO] consul: member '%s' %s, deregistering", member.Name, reason)
	req := structs.DeregisterRequest{
		Datacenter: s.config.Datacenter,
		Node:       member.Name,
	}
	_, err = s.raftApply(structs.DeregisterRequestType, &req)
	return err
}

func (s *Server) joinConsulServer(m serf.Member, parts *metadata.Server) error {

	if parts.Bootstrap {
		members := s.serfLAN.Members()
		for _, member := range members {
			valid, p := metadata.IsConsulServer(member)
			if valid && member.Name != m.Name && p.Bootstrap {
				s.logger.Printf("[ERR] consul: '%v' and '%v' are both in bootstrap mode. Only one node should be in bootstrap mode, not adding Raft peer.", m.Name, member.Name)
				return nil
			}
		}
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("[ERR] consul: failed to get raft configuration: %v", err)
		return err
	}
	if m.Name == s.config.NodeName {
		if l := len(configFuture.Configuration().Servers); l < 3 {
			s.logger.Printf("[DEBUG] consul: Skipping self join check for %q since the cluster is too small", m.Name)
			return nil
		}
	}

	addr := (&net.TCPAddr{IP: m.Addr, Port: parts.Port}).String()
	minRaftProtocol, err := s.autopilot.MinRaftProtocol()
	if err != nil {
		return err
	}
	for _, server := range configFuture.Configuration().Servers {

		if server.Address == raft.ServerAddress(addr) && (minRaftProtocol < 2 || parts.RaftVersion < 3) {
			return nil
		}

		if server.Address == raft.ServerAddress(addr) || server.ID == raft.ServerID(parts.ID) {

			if server.Address == raft.ServerAddress(addr) && server.ID == raft.ServerID(parts.ID) {
				return nil
			}
			future := s.raft.RemoveServer(server.ID, 0, 0)
			if server.Address == raft.ServerAddress(addr) {
				if err := future.Error(); err != nil {
					return fmt.Errorf("error removing server with duplicate address %q: %s", server.Address, err)
				}
				s.logger.Printf("[INFO] consul: removed server with duplicate address: %s", server.Address)
			} else {
				if err := future.Error(); err != nil {
					return fmt.Errorf("error removing server with duplicate ID %q: %s", server.ID, err)
				}
				s.logger.Printf("[INFO] consul: removed server with duplicate ID: %s", server.ID)
			}
		}
	}

	switch {
	case minRaftProtocol >= 3:
		addFuture := s.raft.AddNonvoter(raft.ServerID(parts.ID), raft.ServerAddress(addr), 0, 0)
		if err := addFuture.Error(); err != nil {
			s.logger.Printf("[ERR] consul: failed to add raft peer: %v", err)
			return err
		}
	case minRaftProtocol == 2 && parts.RaftVersion >= 3:
		addFuture := s.raft.AddVoter(raft.ServerID(parts.ID), raft.ServerAddress(addr), 0, 0)
		if err := addFuture.Error(); err != nil {
			s.logger.Printf("[ERR] consul: failed to add raft peer: %v", err)
			return err
		}
	default:
		addFuture := s.raft.AddPeer(raft.ServerAddress(addr))
		if err := addFuture.Error(); err != nil {
			s.logger.Printf("[ERR] consul: failed to add raft peer: %v", err)
			return err
		}
	}

	s.autopilot.RemoveDeadServers()

	return nil
}

func (s *Server) removeConsulServer(m serf.Member, port int) error {
	addr := (&net.TCPAddr{IP: m.Addr, Port: port}).String()

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("[ERR] consul: failed to get raft configuration: %v", err)
		return err
	}

	minRaftProtocol, err := s.autopilot.MinRaftProtocol()
	if err != nil {
		return err
	}

	_, parts := metadata.IsConsulServer(m)

	for _, server := range configFuture.Configuration().Servers {

		if minRaftProtocol >= 2 && server.ID == raft.ServerID(parts.ID) {
			s.logger.Printf("[INFO] consul: removing server by ID: %q", server.ID)
			future := s.raft.RemoveServer(raft.ServerID(parts.ID), 0, 0)
			if err := future.Error(); err != nil {
				s.logger.Printf("[ERR] consul: failed to remove raft peer '%v': %v",
					server.ID, err)
				return err
			}
			break
		} else if server.Address == raft.ServerAddress(addr) {

			s.logger.Printf("[INFO] consul: removing server by address: %q", server.Address)
			future := s.raft.RemovePeer(raft.ServerAddress(addr))
			if err := future.Error(); err != nil {
				s.logger.Printf("[ERR] consul: failed to remove raft peer '%v': %v",
					addr, err)
				return err
			}
			break
		}
	}

	return nil
}

func (s *Server) reapTombstones(index uint64) {
	defer metrics.MeasureSince([]string{"consul", "leader", "reapTombstones"}, time.Now())
	defer metrics.MeasureSince([]string{"leader", "reapTombstones"}, time.Now())
	req := structs.TombstoneRequest{
		Datacenter: s.config.Datacenter,
		Op:         structs.TombstoneReap,
		ReapIndex:  index,
	}
	_, err := s.raftApply(structs.TombstoneRequestType, &req)
	if err != nil {
		s.logger.Printf("[ERR] consul: failed to reap tombstones up to %d: %v",
			index, err)
	}
}
