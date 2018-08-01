package autopilot

import (
	"time"

	"github.com/as/consulrun/hashicorp/serf/serf"
)

type Config struct {
	CleanupDeadServers bool

	LastContactThreshold time.Duration

	MaxTrailingLogs uint64

	ServerStabilizationTime time.Duration

	RedundancyZoneTag string

	DisableUpgradeMigration bool

	UpgradeVersionTag string

	CreateIndex uint64
	ModifyIndex uint64
}

type ServerHealth struct {
	ID string

	Name string

	Address string

	SerfStatus serf.MemberStatus

	Version string

	Leader bool

	LastContact time.Duration

	LastTerm uint64

	LastIndex uint64

	Healthy bool

	Voter bool

	StableSince time.Time
}

func (h *ServerHealth) IsHealthy(lastTerm uint64, leaderLastIndex uint64, autopilotConf *Config) bool {
	if h.SerfStatus != serf.StatusAlive {
		return false
	}

	if h.LastContact > autopilotConf.LastContactThreshold || h.LastContact < 0 {
		return false
	}

	if h.LastTerm != lastTerm {
		return false
	}

	if leaderLastIndex > autopilotConf.MaxTrailingLogs && h.LastIndex < leaderLastIndex-autopilotConf.MaxTrailingLogs {
		return false
	}

	return true
}

func (h *ServerHealth) IsStable(now time.Time, conf *Config) bool {
	if h == nil {
		return false
	}

	if !h.Healthy {
		return false
	}

	if now.Sub(h.StableSince) < conf.ServerStabilizationTime {
		return false
	}

	return true
}

type ServerStats struct {
	LastContact string

	LastTerm uint64

	LastIndex uint64
}

type OperatorHealthReply struct {
	Healthy bool

	FailureTolerance int

	Servers []ServerHealth
}

func (o *OperatorHealthReply) ServerHealth(id string) *ServerHealth {
	for _, health := range o.Servers {
		if health.ID == id {
			return &health
		}
	}
	return nil
}
