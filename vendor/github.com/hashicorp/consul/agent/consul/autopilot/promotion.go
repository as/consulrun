package autopilot

import (
	"time"

	"github.com/hashicorp/raft"
)

func PromoteStableServers(autopilotConfig *Config, health OperatorHealthReply, servers []raft.Server) []raft.Server {

	now := time.Now()
	var promotions []raft.Server
	for _, server := range servers {
		if !IsPotentialVoter(server.Suffrage) {
			health := health.ServerHealth(string(server.ID))
			if health.IsStable(now, autopilotConfig) {
				promotions = append(promotions, server)
			}
		}
	}

	return promotions
}
