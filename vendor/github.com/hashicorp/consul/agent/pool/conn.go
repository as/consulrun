package pool

type RPCType byte

const (
	RPCConsul      RPCType = 0
	RPCRaft                = 1
	RPCMultiplex           = 2 // Old Muxado byte, no longer supported.
	RPCTLS                 = 3
	RPCMultiplexV2         = 4
	RPCSnapshot            = 5
	RPCGossip              = 6
)
