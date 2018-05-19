package lib

import (
	"github.com/hashicorp/serf/serf"
)

func SerfDefaultConfig() *serf.Config {
	base := serf.DefaultConfig()

	base.QueueDepthWarning = 1000000

	base.MinQueueDepth = 4096

	return base
}
