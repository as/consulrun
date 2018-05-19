package serf

import (
	"bytes"
	"net"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
)

type messageType uint8

const (
	messageLeaveType messageType = iota
	messageJoinType
	messagePushPullType
	messageUserEventType
	messageQueryType
	messageQueryResponseType
	messageConflictResponseType
	messageKeyRequestType
	messageKeyResponseType
	messageRelayType
)

const (
	queryFlagAck uint32 = 1 << iota

	queryFlagNoBroadcast
)

type filterType uint8

const (
	filterNodeType filterType = iota
	filterTagType
)

type messageJoin struct {
	LTime LamportTime
	Node  string
}

type messageLeave struct {
	LTime LamportTime
	Node  string
}

type messagePushPull struct {
	LTime        LamportTime            // Current node lamport time
	StatusLTimes map[string]LamportTime // Maps the node to its status time
	LeftMembers  []string               // List of left nodes
	EventLTime   LamportTime            // Lamport time for event clock
	Events       []*userEvents          // Recent events
	QueryLTime   LamportTime            // Lamport time for query clock
}

type messageUserEvent struct {
	LTime   LamportTime
	Name    string
	Payload []byte
	CC      bool // "Can Coalesce". Zero value is compatible with Serf 0.1
}

type messageQuery struct {
	LTime       LamportTime   // Event lamport time
	ID          uint32        // Query ID, randomly generated
	Addr        []byte        // Source address, used for a direct reply
	Port        uint16        // Source port, used for a direct reply
	Filters     [][]byte      // Potential query filters
	Flags       uint32        // Used to provide various flags
	RelayFactor uint8         // Used to set the number of duplicate relayed responses
	Timeout     time.Duration // Maximum time between delivery and response
	Name        string        // Query name
	Payload     []byte        // Query payload
}

func (m *messageQuery) Ack() bool {
	return (m.Flags & queryFlagAck) != 0
}

func (m *messageQuery) NoBroadcast() bool {
	return (m.Flags & queryFlagNoBroadcast) != 0
}

type filterNode []string

type filterTag struct {
	Tag  string
	Expr string
}

type messageQueryResponse struct {
	LTime   LamportTime // Event lamport time
	ID      uint32      // Query ID
	From    string      // Node name
	Flags   uint32      // Used to provide various flags
	Payload []byte      // Optional response payload
}

func (m *messageQueryResponse) Ack() bool {
	return (m.Flags & queryFlagAck) != 0
}

func decodeMessage(buf []byte, out interface{}) error {
	var handle codec.MsgpackHandle
	return codec.NewDecoder(bytes.NewReader(buf), &handle).Decode(out)
}

func encodeMessage(t messageType, msg interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(uint8(t))

	handle := codec.MsgpackHandle{}
	encoder := codec.NewEncoder(buf, &handle)
	err := encoder.Encode(msg)
	return buf.Bytes(), err
}

type relayHeader struct {
	DestAddr net.UDPAddr
}

func encodeRelayMessage(t messageType, addr net.UDPAddr, msg interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	handle := codec.MsgpackHandle{}
	encoder := codec.NewEncoder(buf, &handle)

	buf.WriteByte(uint8(messageRelayType))
	if err := encoder.Encode(relayHeader{DestAddr: addr}); err != nil {
		return nil, err
	}

	buf.WriteByte(uint8(t))
	err := encoder.Encode(msg)
	return buf.Bytes(), err
}

func encodeFilter(f filterType, filt interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(uint8(f))

	handle := codec.MsgpackHandle{}
	encoder := codec.NewEncoder(buf, &handle)
	err := encoder.Encode(filt)
	return buf.Bytes(), err
}
