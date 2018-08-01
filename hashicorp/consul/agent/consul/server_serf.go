package consul

import (
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/as/consulrun/hashicorp/consul/agent/metadata"
	"github.com/as/consulrun/hashicorp/consul/lib"
	"github.com/as/consulrun/hashicorp/raft"
	"github.com/as/consulrun/hashicorp/serf/serf"
)

const (
	StatusReap = serf.MemberStatus(-1)

	userEventPrefix = "consul:event:"

	maxPeerRetries = 6

	peerRetryBase = 1 * time.Second
)

func (s *Server) setupSerf(conf *serf.Config, ch chan serf.Event, path string, wan bool, wanPort int,
	segment string, listener net.Listener) (*serf.Serf, error) {
	conf.Init()

	if wan {
		conf.NodeName = fmt.Sprintf("%s.%s", s.config.NodeName, s.config.Datacenter)
	} else {
		conf.NodeName = s.config.NodeName
		if wanPort > 0 {
			conf.Tags["wan_join_port"] = fmt.Sprintf("%d", wanPort)
		}
	}
	conf.Tags["role"] = "consul"
	conf.Tags["dc"] = s.config.Datacenter
	conf.Tags["segment"] = segment
	if segment == "" {
		for _, s := range s.config.Segments {
			conf.Tags["sl_"+s.Name] = net.JoinHostPort(s.Advertise, fmt.Sprintf("%d", s.Port))
		}
	}
	conf.Tags["id"] = string(s.config.NodeID)
	conf.Tags["vsn"] = fmt.Sprintf("%d", s.config.ProtocolVersion)
	conf.Tags["vsn_min"] = fmt.Sprintf("%d", ProtocolVersionMin)
	conf.Tags["vsn_max"] = fmt.Sprintf("%d", ProtocolVersionMax)
	conf.Tags["raft_vsn"] = fmt.Sprintf("%d", s.config.RaftConfig.ProtocolVersion)
	conf.Tags["build"] = s.config.Build
	addr := listener.Addr().(*net.TCPAddr)
	conf.Tags["port"] = fmt.Sprintf("%d", addr.Port)
	if s.config.Bootstrap {
		conf.Tags["bootstrap"] = "1"
	}
	if s.config.BootstrapExpect != 0 {
		conf.Tags["expect"] = fmt.Sprintf("%d", s.config.BootstrapExpect)
	}
	if s.config.NonVoter {
		conf.Tags["nonvoter"] = "1"
	}
	if s.config.UseTLS {
		conf.Tags["use_tls"] = "1"
	}
	if s.logger == nil {
		conf.MemberlistConfig.LogOutput = s.config.LogOutput
		conf.LogOutput = s.config.LogOutput
	}
	conf.MemberlistConfig.Logger = s.logger
	conf.Logger = s.logger
	conf.EventCh = ch
	conf.ProtocolVersion = protocolVersionMap[s.config.ProtocolVersion]
	conf.RejoinAfterLeave = s.config.RejoinAfterLeave
	if wan {
		conf.Merge = &wanMergeDelegate{}
	} else {
		conf.Merge = &lanMergeDelegate{
			dc:       s.config.Datacenter,
			nodeID:   s.config.NodeID,
			nodeName: s.config.NodeName,
			segment:  segment,
		}
	}

	conf.EnableNameConflictResolution = false

	if !s.config.DevMode {
		conf.SnapshotPath = filepath.Join(s.config.DataDir, path)
	}
	if err := lib.EnsurePath(conf.SnapshotPath, false); err != nil {
		return nil, err
	}

	return serf.Create(conf)
}

func userEventName(name string) string {
	return userEventPrefix + name
}

func isUserEvent(name string) bool {
	return strings.HasPrefix(name, userEventPrefix)
}

func rawUserEventName(name string) string {
	return strings.TrimPrefix(name, userEventPrefix)
}

func (s *Server) lanEventHandler() {
	for {
		select {
		case e := <-s.eventChLAN:
			switch e.EventType() {
			case serf.EventMemberJoin:
				s.lanNodeJoin(e.(serf.MemberEvent))
				s.localMemberEvent(e.(serf.MemberEvent))

			case serf.EventMemberLeave, serf.EventMemberFailed:
				s.lanNodeFailed(e.(serf.MemberEvent))
				s.localMemberEvent(e.(serf.MemberEvent))

			case serf.EventMemberReap:
				s.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventUser:
				s.localEvent(e.(serf.UserEvent))
			case serf.EventMemberUpdate:
				s.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventQuery: // Ignore
			default:
				s.logger.Printf("[WARN] consul: Unhandled LAN Serf Event: %#v", e)
			}

		case <-s.shutdownCh:
			return
		}
	}
}

func (s *Server) localMemberEvent(me serf.MemberEvent) {

	if !s.IsLeader() {
		return
	}

	isReap := me.EventType() == serf.EventMemberReap

	for _, m := range me.Members {

		if isReap {
			m.Status = StatusReap
		}
		select {
		case s.reconcileCh <- m:
		default:
		}
	}
}

func (s *Server) localEvent(event serf.UserEvent) {

	if !strings.HasPrefix(event.Name, "consul:") {
		return
	}

	switch name := event.Name; {
	case name == newLeaderEvent:
		s.logger.Printf("[INFO] consul: New leader elected: %s", event.Payload)

		if s.config.ServerUp != nil {
			s.config.ServerUp()
		}
	case isUserEvent(name):
		event.Name = rawUserEventName(name)
		s.logger.Printf("[DEBUG] consul: User event: %s", event.Name)

		if s.config.UserEventHandler != nil {
			s.config.UserEventHandler(event)
		}
	default:
		s.logger.Printf("[WARN] consul: Unhandled local event: %v", event)
	}
}

func (s *Server) lanNodeJoin(me serf.MemberEvent) {
	for _, m := range me.Members {
		ok, serverMeta := metadata.IsConsulServer(m)
		if !ok || serverMeta.Segment != "" {
			continue
		}
		s.logger.Printf("[INFO] consul: Adding LAN server %s", serverMeta)

		s.serverLookup.AddServer(serverMeta)

		if s.config.BootstrapExpect != 0 {
			s.maybeBootstrap()
		}

		s.FloodNotify()
	}
}

func (s *Server) maybeBootstrap() {

	index, err := s.raftStore.LastIndex()
	if err != nil {
		s.logger.Printf("[ERR] consul: Failed to read last raft index: %v", err)
		return
	}
	if index != 0 {
		s.logger.Printf("[INFO] consul: Raft data found, disabling bootstrap mode")
		s.config.BootstrapExpect = 0
		return
	}

	members := s.serfLAN.Members()
	var servers []metadata.Server
	for _, member := range members {
		valid, p := metadata.IsConsulServer(member)
		if !valid {
			continue
		}
		if p.Datacenter != s.config.Datacenter {
			s.logger.Printf("[ERR] consul: Member %v has a conflicting datacenter, ignoring", member)
			continue
		}
		if p.Expect != 0 && p.Expect != s.config.BootstrapExpect {
			s.logger.Printf("[ERR] consul: Member %v has a conflicting expect value. All nodes should expect the same number.", member)
			return
		}
		if p.Bootstrap {
			s.logger.Printf("[ERR] consul: Member %v has bootstrap mode. Expect disabled.", member)
			return
		}
		servers = append(servers, *p)
	}

	if len(servers) < s.config.BootstrapExpect {
		return
	}

	for _, server := range servers {
		var peers []string

		for attempt := uint(0); attempt < maxPeerRetries; attempt++ {
			if err := s.connPool.RPC(s.config.Datacenter, server.Addr, server.Version,
				"Status.Peers", server.UseTLS, &struct{}{}, &peers); err != nil {
				nextRetry := time.Duration((1 << attempt) * peerRetryBase)
				s.logger.Printf("[ERR] consul: Failed to confirm peer status for %s: %v. Retrying in "+
					"%v...", server.Name, err, nextRetry.String())
				time.Sleep(nextRetry)
			} else {
				break
			}
		}

		if len(peers) > 0 {
			s.logger.Printf("[INFO] consul: Existing Raft peers reported by %s, disabling bootstrap mode", server.Name)
			s.config.BootstrapExpect = 0
			return
		}
	}

	var configuration raft.Configuration
	var addrs []string
	minRaftVersion, err := s.autopilot.MinRaftProtocol()
	if err != nil {
		s.logger.Printf("[ERR] consul: Failed to read server raft versions: %v", err)
	}

	for _, server := range servers {
		addr := server.Addr.String()
		addrs = append(addrs, addr)
		var id raft.ServerID
		if minRaftVersion >= 3 {
			id = raft.ServerID(server.ID)
		} else {
			id = raft.ServerID(addr)
		}
		peer := raft.Server{
			ID:      id,
			Address: raft.ServerAddress(addr),
		}
		configuration.Servers = append(configuration.Servers, peer)
	}
	s.logger.Printf("[INFO] consul: Found expected number of peers, attempting bootstrap: %s",
		strings.Join(addrs, ","))
	future := s.raft.BootstrapCluster(configuration)
	if err := future.Error(); err != nil {
		s.logger.Printf("[ERR] consul: Failed to bootstrap cluster: %v", err)
	}

	s.config.BootstrapExpect = 0
}

func (s *Server) lanNodeFailed(me serf.MemberEvent) {
	for _, m := range me.Members {
		ok, serverMeta := metadata.IsConsulServer(m)
		if !ok || serverMeta.Segment != "" {
			continue
		}
		s.logger.Printf("[INFO] consul: Removing LAN server %s", serverMeta)

		s.serverLookup.RemoveServer(serverMeta)
	}
}
