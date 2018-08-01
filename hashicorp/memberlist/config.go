package memberlist

import (
	"io"
	"log"
	"os"
	"time"
)

type Config struct {
	Name string

	Transport Transport

	BindAddr string
	BindPort int

	AdvertiseAddr string
	AdvertisePort int

	ProtocolVersion uint8

	TCPTimeout time.Duration

	IndirectChecks int

	//

	//

	RetransmitMult int

	//

	//

	SuspicionMult int

	//

	//

	SuspicionMaxTimeoutMult int

	//

	PushPullInterval time.Duration

	//

	//

	ProbeInterval time.Duration
	ProbeTimeout  time.Duration

	DisableTcpPings bool

	AwarenessMaxMultiplier int

	//

	//

	//

	GossipInterval      time.Duration
	GossipNodes         int
	GossipToTheDeadTime time.Duration

	GossipVerifyIncoming bool

	GossipVerifyOutgoing bool

	EnableCompression bool

	SecretKey []byte

	Keyring *Keyring

	//

	Delegate                Delegate
	DelegateProtocolVersion uint8
	DelegateProtocolMin     uint8
	DelegateProtocolMax     uint8
	Events                  EventDelegate
	Conflict                ConflictDelegate
	Merge                   MergeDelegate
	Ping                    PingDelegate
	Alive                   AliveDelegate

	DNSConfigPath string

	LogOutput io.Writer

	Logger *log.Logger

	HandoffQueueDepth int

	UDPBufferSize int
}

func DefaultLANConfig() *Config {
	hostname, _ := os.Hostname()
	return &Config{
		Name:                    hostname,
		BindAddr:                "0.0.0.0",
		BindPort:                7946,
		AdvertiseAddr:           "",
		AdvertisePort:           7946,
		ProtocolVersion:         ProtocolVersion2Compatible,
		TCPTimeout:              10 * time.Second,       // Timeout after 10 seconds
		IndirectChecks:          3,                      // Use 3 nodes for the indirect ping
		RetransmitMult:          4,                      // Retransmit a message 4 * log(N+1) nodes
		SuspicionMult:           4,                      // Suspect a node for 4 * log(N+1) * Interval
		SuspicionMaxTimeoutMult: 6,                      // For 10k nodes this will give a max timeout of 120 seconds
		PushPullInterval:        30 * time.Second,       // Low frequency
		ProbeTimeout:            500 * time.Millisecond, // Reasonable RTT time for LAN
		ProbeInterval:           1 * time.Second,        // Failure check every second
		DisableTcpPings:         false,                  // TCP pings are safe, even with mixed versions
		AwarenessMaxMultiplier:  8,                      // Probe interval backs off to 8 seconds

		GossipNodes:          3,                      // Gossip to 3 nodes
		GossipInterval:       200 * time.Millisecond, // Gossip more rapidly
		GossipToTheDeadTime:  30 * time.Second,       // Same as push/pull
		GossipVerifyIncoming: true,
		GossipVerifyOutgoing: true,

		EnableCompression: true, // Enable compression by default

		SecretKey: nil,
		Keyring:   nil,

		DNSConfigPath: "/etc/resolv.conf",

		HandoffQueueDepth: 1024,
		UDPBufferSize:     1400,
	}
}

func DefaultWANConfig() *Config {
	conf := DefaultLANConfig()
	conf.TCPTimeout = 30 * time.Second
	conf.SuspicionMult = 6
	conf.PushPullInterval = 60 * time.Second
	conf.ProbeTimeout = 3 * time.Second
	conf.ProbeInterval = 5 * time.Second
	conf.GossipNodes = 4 // Gossip less frequently, but to an additional node
	conf.GossipInterval = 500 * time.Millisecond
	conf.GossipToTheDeadTime = 60 * time.Second
	return conf
}

func DefaultLocalConfig() *Config {
	conf := DefaultLANConfig()
	conf.TCPTimeout = time.Second
	conf.IndirectChecks = 1
	conf.RetransmitMult = 2
	conf.SuspicionMult = 3
	conf.PushPullInterval = 15 * time.Second
	conf.ProbeTimeout = 200 * time.Millisecond
	conf.ProbeInterval = time.Second
	conf.GossipInterval = 100 * time.Millisecond
	conf.GossipToTheDeadTime = 15 * time.Second
	return conf
}

func (c *Config) EncryptionEnabled() bool {
	return c.Keyring != nil && len(c.Keyring.GetKeys()) > 0
}
