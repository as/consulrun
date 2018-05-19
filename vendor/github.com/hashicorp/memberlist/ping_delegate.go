package memberlist

import "time"

type PingDelegate interface {
	AckPayload() []byte

	NotifyPingComplete(other *Node, rtt time.Duration, payload []byte)
}
