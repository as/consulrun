package raft

import (
	"fmt"
	"io"
	"log"
	"time"
)

//
//
//
//
//
//
//
//
type ProtocolVersion int

const (
	ProtocolVersionMin ProtocolVersion = 0
	ProtocolVersionMax                 = 3
)

//
//
type SnapshotVersion int

const (
	SnapshotVersionMin SnapshotVersion = 0
	SnapshotVersionMax                 = 1
)

type Config struct {
	ProtocolVersion ProtocolVersion

	HeartbeatTimeout time.Duration

	ElectionTimeout time.Duration

	CommitTimeout time.Duration

	MaxAppendEntries int

	ShutdownOnRemove bool

	TrailingLogs uint64

	SnapshotInterval time.Duration

	SnapshotThreshold uint64

	LeaderLeaseTimeout time.Duration

	StartAsLeader bool

	LocalID ServerID

	NotifyCh chan<- bool

	LogOutput io.Writer

	Logger *log.Logger
}

func DefaultConfig() *Config {
	return &Config{
		ProtocolVersion:    ProtocolVersionMax,
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		MaxAppendEntries:   64,
		ShutdownOnRemove:   true,
		TrailingLogs:       10240,
		SnapshotInterval:   120 * time.Second,
		SnapshotThreshold:  8192,
		LeaderLeaseTimeout: 500 * time.Millisecond,
	}
}

func ValidateConfig(config *Config) error {

	protocolMin := ProtocolVersionMin
	if protocolMin == 0 {
		protocolMin = 1
	}
	if config.ProtocolVersion < protocolMin ||
		config.ProtocolVersion > ProtocolVersionMax {
		return fmt.Errorf("Protocol version %d must be >= %d and <= %d",
			config.ProtocolVersion, protocolMin, ProtocolVersionMax)
	}
	if len(config.LocalID) == 0 {
		return fmt.Errorf("LocalID cannot be empty")
	}
	if config.HeartbeatTimeout < 5*time.Millisecond {
		return fmt.Errorf("Heartbeat timeout is too low")
	}
	if config.ElectionTimeout < 5*time.Millisecond {
		return fmt.Errorf("Election timeout is too low")
	}
	if config.CommitTimeout < time.Millisecond {
		return fmt.Errorf("Commit timeout is too low")
	}
	if config.MaxAppendEntries <= 0 {
		return fmt.Errorf("MaxAppendEntries must be positive")
	}
	if config.MaxAppendEntries > 1024 {
		return fmt.Errorf("MaxAppendEntries is too large")
	}
	if config.SnapshotInterval < 5*time.Millisecond {
		return fmt.Errorf("Snapshot interval is too low")
	}
	if config.LeaderLeaseTimeout < 5*time.Millisecond {
		return fmt.Errorf("Leader lease timeout is too low")
	}
	if config.LeaderLeaseTimeout > config.HeartbeatTimeout {
		return fmt.Errorf("Leader lease timeout cannot be larger than heartbeat timeout")
	}
	if config.ElectionTimeout < config.HeartbeatTimeout {
		return fmt.Errorf("Election timeout must be equal or greater than Heartbeat Timeout")
	}
	return nil
}
