package serf

import (
	"io"
	"log"
	"os"
	"time"

	"github.com/as/consulrun/hashicorp/memberlist"
)

var ProtocolVersionMap map[uint8]uint8

func init() {
	ProtocolVersionMap = map[uint8]uint8{
		5: 2,
		4: 2,
		3: 2,
		2: 2,
	}
}

type Config struct {
	NodeName string

	Tags map[string]string

	EventCh chan<- Event

	ProtocolVersion uint8

	BroadcastTimeout time.Duration

	//

	//

	//

	CoalescePeriod  time.Duration
	QuiescentPeriod time.Duration

	UserCoalescePeriod  time.Duration
	UserQuiescentPeriod time.Duration

	//

	//

	//

	//

	ReapInterval      time.Duration
	ReconnectInterval time.Duration
	ReconnectTimeout  time.Duration
	TombstoneTimeout  time.Duration

	FlapTimeout time.Duration

	QueueCheckInterval time.Duration

	QueueDepthWarning int

	MaxQueueDepth int

	MinQueueDepth int

	RecentIntentTimeout time.Duration

	EventBuffer int

	QueryBuffer int

	//

	//
	QueryTimeoutMult int

	QueryResponseSizeLimit int
	QuerySizeLimit         int

	//

	//

	//

	//
	MemberlistConfig *memberlist.Config

	LogOutput io.Writer

	Logger *log.Logger

	SnapshotPath string

	RejoinAfterLeave bool

	EnableNameConflictResolution bool

	DisableCoordinates bool

	KeyringFile string

	Merge MergeDelegate
}

func (c *Config) Init() {
	if c.Tags == nil {
		c.Tags = make(map[string]string)
	}
}

func DefaultConfig() *Config {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	return &Config{
		NodeName:                     hostname,
		BroadcastTimeout:             5 * time.Second,
		EventBuffer:                  512,
		QueryBuffer:                  512,
		LogOutput:                    os.Stderr,
		ProtocolVersion:              4,
		ReapInterval:                 15 * time.Second,
		RecentIntentTimeout:          5 * time.Minute,
		ReconnectInterval:            30 * time.Second,
		ReconnectTimeout:             24 * time.Hour,
		QueueCheckInterval:           30 * time.Second,
		QueueDepthWarning:            128,
		MaxQueueDepth:                4096,
		TombstoneTimeout:             24 * time.Hour,
		FlapTimeout:                  60 * time.Second,
		MemberlistConfig:             memberlist.DefaultLANConfig(),
		QueryTimeoutMult:             16,
		QueryResponseSizeLimit:       1024,
		QuerySizeLimit:               1024,
		EnableNameConflictResolution: true,
		DisableCoordinates:           false,
	}
}
