package consul

import (
	"fmt"
	"strconv"

	"github.com/as/consulrun/hashicorp/consul/agent/consul/autopilot"
)

type Status struct {
	server *Server
}

func (s *Status) Ping(args struct{}, reply *struct{}) error {
	return nil
}

func (s *Status) Leader(args struct{}, reply *string) error {
	leader := string(s.server.raft.Leader())
	if leader != "" {
		*reply = leader
	} else {
		*reply = ""
	}
	return nil
}

func (s *Status) Peers(args struct{}, reply *[]string) error {
	future := s.server.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return err
	}

	for _, server := range future.Configuration().Servers {
		*reply = append(*reply, string(server.Address))
	}
	return nil
}

func (s *Status) RaftStats(args struct{}, reply *autopilot.ServerStats) error {
	stats := s.server.raft.Stats()

	var err error
	reply.LastContact = stats["last_contact"]
	reply.LastIndex, err = strconv.ParseUint(stats["last_log_index"], 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing server's last_log_index value: %s", err)
	}
	reply.LastTerm, err = strconv.ParseUint(stats["last_log_term"], 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing server's last_log_term value: %s", err)
	}

	return nil
}
