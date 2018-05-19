package sockaddr

import (
	"encoding/json"
	"fmt"
	"strings"
)

type SockAddrType int
type AttrName string

const (
	TypeUnknown SockAddrType = 0x0
	TypeUnix                 = 0x1
	TypeIPv4                 = 0x2
	TypeIPv6                 = 0x4

	TypeIP = 0x6
)

type SockAddr interface {
	CmpRFC(rfcNum uint, sa SockAddr) int

	Contains(SockAddr) bool

	Equal(SockAddr) bool

	DialPacketArgs() (string, string)
	DialStreamArgs() (string, string)
	ListenPacketArgs() (string, string)
	ListenStreamArgs() (string, string)

	String() string

	Type() SockAddrType
}

var sockAddrAttrMap map[AttrName]func(SockAddr) string
var sockAddrAttrs []AttrName

func init() {
	sockAddrInit()
}

//
func NewSockAddr(s string) (SockAddr, error) {
	ipv4Addr, err := NewIPv4Addr(s)
	if err == nil {
		return ipv4Addr, nil
	}

	ipv6Addr, err := NewIPv6Addr(s)
	if err == nil {
		return ipv6Addr, nil
	}

	if len(s) > 1 && (strings.IndexAny(s[0:1], "./") != -1 || strings.IndexByte(s, '/') != -1) {
		unixSock, err := NewUnixSock(s)
		if err == nil {
			return unixSock, nil
		}
	}

	return nil, fmt.Errorf("Unable to convert %q to an IPv4 or IPv6 address, or a UNIX Socket", s)
}

func ToIPAddr(sa SockAddr) *IPAddr {
	ipa, ok := sa.(IPAddr)
	if !ok {
		return nil
	}
	return &ipa
}

func ToIPv4Addr(sa SockAddr) *IPv4Addr {
	switch v := sa.(type) {
	case IPv4Addr:
		return &v
	default:
		return nil
	}
}

func ToIPv6Addr(sa SockAddr) *IPv6Addr {
	switch v := sa.(type) {
	case IPv6Addr:
		return &v
	default:
		return nil
	}
}

func ToUnixSock(sa SockAddr) *UnixSock {
	switch v := sa.(type) {
	case UnixSock:
		return &v
	default:
		return nil
	}
}

func SockAddrAttr(sa SockAddr, selector AttrName) string {
	fn, found := sockAddrAttrMap[selector]
	if !found {
		return ""
	}

	return fn(sa)
}

func (sat SockAddrType) String() string {
	switch sat {
	case TypeIPv4:
		return "IPv4"
	case TypeIPv6:
		return "IPv6"

	case TypeUnix:
		return "UNIX"
	default:
		panic("unsupported type")
	}
}

func sockAddrInit() {
	sockAddrAttrs = []AttrName{
		"type", // type should be first
		"string",
	}

	sockAddrAttrMap = map[AttrName]func(sa SockAddr) string{
		"string": func(sa SockAddr) string {
			return sa.String()
		},
		"type": func(sa SockAddr) string {
			return sa.Type().String()
		},
	}
}

func SockAddrAttrs() []AttrName {
	return sockAddrAttrs
}

type SockAddrMarshaler struct {
	SockAddr
}

func (s *SockAddrMarshaler) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.SockAddr.String())
}

func (s *SockAddrMarshaler) UnmarshalJSON(in []byte) error {
	var str string
	err := json.Unmarshal(in, &str)
	if err != nil {
		return err
	}
	sa, err := NewSockAddr(str)
	if err != nil {
		return err
	}
	s.SockAddr = sa
	return nil
}
