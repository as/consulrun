// +build !ent

package consul

import (
	"net"
	"time"

	"github.com/armon/go-metrics"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/serf/serf"
)

func (s *Server) LANMembersAllSegments() ([]serf.Member, error) {
	return s.LANMembers(), nil
}

func (s *Server) LANSegmentMembers(segment string) ([]serf.Member, error) {
	if segment == "" {
		return s.LANMembers(), nil
	}

	return nil, structs.ErrSegmentsNotSupported
}

func (s *Server) LANSegmentAddr(name string) string {
	return ""
}

func (s *Server) setupSegmentRPC() (map[string]net.Listener, error) {
	if len(s.config.Segments) > 0 {
		return nil, structs.ErrSegmentsNotSupported
	}

	return nil, nil
}

func (s *Server) setupSegments(config *Config, port int, rpcListeners map[string]net.Listener) error {
	if len(config.Segments) > 0 {
		return structs.ErrSegmentsNotSupported
	}

	return nil
}

func (s *Server) floodSegments(config *Config) {
}

func (s *Server) reconcile() (err error) {
	defer metrics.MeasureSince([]string{"consul", "leader", "reconcile"}, time.Now())
	defer metrics.MeasureSince([]string{"leader", "reconcile"}, time.Now())
	members := s.serfLAN.Members()
	knownMembers := make(map[string]struct{})
	for _, member := range members {
		if err := s.reconcileMember(member); err != nil {
			return err
		}
		knownMembers[member.Name] = struct{}{}
	}

	return s.reconcileReaped(knownMembers)
}
