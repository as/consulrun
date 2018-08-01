package consul

import (
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/as/consulrun/hashicorp/consul/agent/consul/autopilot"
	"github.com/as/consulrun/hashicorp/consul/lib"
	"github.com/as/consulrun/hashicorp/consul/tlsutil"
	"github.com/as/consulrun/hashicorp/consul/types"
	"github.com/as/consulrun/hashicorp/consul/version"
	"github.com/as/consulrun/hashicorp/memberlist"
	"github.com/as/consulrun/hashicorp/raft"
	"github.com/as/consulrun/hashicorp/serf/serf"
	"golang.org/x/time/rate"
)

const (
	DefaultDC          = "dc1"
	DefaultRPCPort     = 8300
	DefaultLANSerfPort = 8301
	DefaultWANSerfPort = 8302

	DefaultRaftMultiplier uint = 5

	MaxRaftMultiplier uint = 10
)

var (
	DefaultRPCAddr = &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: DefaultRPCPort}

	protocolVersionMap map[uint8]uint8
)

func init() {
	protocolVersionMap = map[uint8]uint8{
		1: 4,
		2: 4,
		3: 4,
	}
}

type NetworkSegment struct {
	Name       string
	Bind       string
	Port       int
	Advertise  string
	RPCAddr    *net.TCPAddr
	SerfConfig *serf.Config
}

type Config struct {
	Bootstrap bool

	BootstrapExpect int

	Datacenter string

	DataDir string

	DevMode bool

	NodeID types.NodeID

	NodeName string

	Domain string

	RaftConfig *raft.Config

	NonVoter bool

	NotifyListen func()

	RPCAddr *net.TCPAddr

	RPCAdvertise *net.TCPAddr

	RPCSrcAddr *net.TCPAddr

	Segment string

	Segments []NetworkSegment

	SerfLANConfig *serf.Config

	SerfWANConfig *serf.Config

	SerfFloodInterval time.Duration

	ReconcileInterval time.Duration

	LogOutput io.Writer

	ProtocolVersion uint8

	VerifyIncoming bool

	VerifyOutgoing bool

	UseTLS bool

	VerifyServerHostname bool

	CAFile string

	CAPath string

	CertFile string

	KeyFile string

	ServerName string

	TLSMinVersion string

	TLSCipherSuites []uint16

	TLSPreferServerCipherSuites bool

	RejoinAfterLeave bool

	Build string

	ACLMasterToken string

	ACLDatacenter string

	ACLTTL time.Duration

	ACLDefaultPolicy string

	ACLDownPolicy string

	EnableACLReplication bool

	ACLReplicationInterval time.Duration

	ACLReplicationApplyLimit int

	ACLEnforceVersion8 bool

	ACLEnableKeyListPolicy bool

	//

	//
	TombstoneTTL time.Duration

	TombstoneTTLGranularity time.Duration

	SessionTTLMin time.Duration

	ServerUp func()

	UserEventHandler func(serf.UserEvent)

	CoordinateUpdatePeriod time.Duration

	CoordinateUpdateBatchSize int

	CoordinateUpdateMaxBatches int

	RPCHoldTimeout time.Duration

	//

	RPCRate     rate.Limit
	RPCMaxBurst int

	LeaveDrainTime time.Duration

	AutopilotConfig *autopilot.Config

	ServerHealthInterval time.Duration

	AutopilotInterval time.Duration
}

func (c *Config) CheckProtocolVersion() error {
	if c.ProtocolVersion < ProtocolVersionMin {
		return fmt.Errorf("Protocol version '%d' too low. Must be in range: [%d, %d]", c.ProtocolVersion, ProtocolVersionMin, ProtocolVersionMax)
	}
	if c.ProtocolVersion > ProtocolVersionMax {
		return fmt.Errorf("Protocol version '%d' too high. Must be in range: [%d, %d]", c.ProtocolVersion, ProtocolVersionMin, ProtocolVersionMax)
	}
	return nil
}

func (c *Config) CheckACL() error {
	switch c.ACLDefaultPolicy {
	case "allow":
	case "deny":
	default:
		return fmt.Errorf("Unsupported default ACL policy: %s", c.ACLDefaultPolicy)
	}
	switch c.ACLDownPolicy {
	case "allow":
	case "deny":
	case "extend-cache":
	default:
		return fmt.Errorf("Unsupported down ACL policy: %s", c.ACLDownPolicy)
	}
	return nil
}

func DefaultConfig() *Config {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	conf := &Config{
		Build:                    version.Version,
		Datacenter:               DefaultDC,
		NodeName:                 hostname,
		RPCAddr:                  DefaultRPCAddr,
		RaftConfig:               raft.DefaultConfig(),
		SerfLANConfig:            lib.SerfDefaultConfig(),
		SerfWANConfig:            lib.SerfDefaultConfig(),
		SerfFloodInterval:        60 * time.Second,
		ReconcileInterval:        60 * time.Second,
		ProtocolVersion:          ProtocolVersion2Compatible,
		ACLTTL:                   30 * time.Second,
		ACLDefaultPolicy:         "allow",
		ACLDownPolicy:            "extend-cache",
		ACLReplicationInterval:   30 * time.Second,
		ACLReplicationApplyLimit: 100, // ops / sec
		TombstoneTTL:             15 * time.Minute,
		TombstoneTTLGranularity:  30 * time.Second,
		SessionTTLMin:            10 * time.Second,

		CoordinateUpdatePeriod:     5 * time.Second,
		CoordinateUpdateBatchSize:  128,
		CoordinateUpdateMaxBatches: 5,

		RPCRate:     rate.Inf,
		RPCMaxBurst: 1000,

		TLSMinVersion: "tls10",

		AutopilotConfig: &autopilot.Config{
			CleanupDeadServers:      true,
			LastContactThreshold:    200 * time.Millisecond,
			MaxTrailingLogs:         250,
			ServerStabilizationTime: 10 * time.Second,
		},

		ServerHealthInterval: 2 * time.Second,
		AutopilotInterval:    10 * time.Second,
	}

	conf.SerfLANConfig.ReconnectTimeout = 3 * 24 * time.Hour
	conf.SerfWANConfig.ReconnectTimeout = 3 * 24 * time.Hour

	conf.SerfWANConfig.MemberlistConfig = memberlist.DefaultWANConfig()

	conf.SerfLANConfig.MemberlistConfig.BindPort = DefaultLANSerfPort
	conf.SerfWANConfig.MemberlistConfig.BindPort = DefaultWANSerfPort

	conf.RaftConfig.ProtocolVersion = 3

	conf.RaftConfig.ShutdownOnRemove = false

	conf.RaftConfig.SnapshotInterval = 5 * time.Second

	return conf
}

func (c *Config) tlsConfig() *tlsutil.Config {
	tlsConf := &tlsutil.Config{
		VerifyIncoming:           c.VerifyIncoming,
		VerifyOutgoing:           c.VerifyOutgoing,
		VerifyServerHostname:     c.VerifyServerHostname,
		UseTLS:                   c.UseTLS,
		CAFile:                   c.CAFile,
		CAPath:                   c.CAPath,
		CertFile:                 c.CertFile,
		KeyFile:                  c.KeyFile,
		NodeName:                 c.NodeName,
		ServerName:               c.ServerName,
		Domain:                   c.Domain,
		TLSMinVersion:            c.TLSMinVersion,
		PreferServerCipherSuites: c.TLSPreferServerCipherSuites,
	}
	return tlsConf
}
