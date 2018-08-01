package yamux

import (
	"encoding/binary"
	"fmt"
)

var (
	ErrInvalidVersion = fmt.Errorf("invalid protocol version")

	ErrInvalidMsgType = fmt.Errorf("invalid msg type")

	ErrSessionShutdown = fmt.Errorf("session shutdown")

	ErrStreamsExhausted = fmt.Errorf("streams exhausted")

	ErrDuplicateStream = fmt.Errorf("duplicate stream initiated")

	ErrRecvWindowExceeded = fmt.Errorf("recv window exceeded")

	ErrTimeout = fmt.Errorf("i/o deadline reached")

	ErrStreamClosed = fmt.Errorf("stream closed")

	ErrUnexpectedFlag = fmt.Errorf("unexpected flag")

	ErrRemoteGoAway = fmt.Errorf("remote end is not accepting connections")

	ErrConnectionReset = fmt.Errorf("connection reset")

	ErrConnectionWriteTimeout = fmt.Errorf("connection write timeout")

	ErrKeepAliveTimeout = fmt.Errorf("keepalive timeout")
)

const (
	protoVersion uint8 = 0
)

const (
	typeData uint8 = iota

	typeWindowUpdate

	typePing

	typeGoAway
)

const (
	flagSYN uint16 = 1 << iota

	flagACK

	flagFIN

	flagRST
)

const (
	initialStreamWindow uint32 = 256 * 1024
)

const (
	goAwayNormal uint32 = iota

	goAwayProtoErr

	goAwayInternalErr
)

const (
	sizeOfVersion  = 1
	sizeOfType     = 1
	sizeOfFlags    = 2
	sizeOfStreamID = 4
	sizeOfLength   = 4
	headerSize     = sizeOfVersion + sizeOfType + sizeOfFlags +
		sizeOfStreamID + sizeOfLength
)

type header []byte

func (h header) Version() uint8 {
	return h[0]
}

func (h header) MsgType() uint8 {
	return h[1]
}

func (h header) Flags() uint16 {
	return binary.BigEndian.Uint16(h[2:4])
}

func (h header) StreamID() uint32 {
	return binary.BigEndian.Uint32(h[4:8])
}

func (h header) Length() uint32 {
	return binary.BigEndian.Uint32(h[8:12])
}

func (h header) String() string {
	return fmt.Sprintf("Vsn:%d Type:%d Flags:%d StreamID:%d Length:%d",
		h.Version(), h.MsgType(), h.Flags(), h.StreamID(), h.Length())
}

func (h header) encode(msgType uint8, flags uint16, streamID uint32, length uint32) {
	h[0] = protoVersion
	h[1] = msgType
	binary.BigEndian.PutUint16(h[2:4], flags)
	binary.BigEndian.PutUint32(h[4:8], streamID)
	binary.BigEndian.PutUint32(h[8:12], length)
}
