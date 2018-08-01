package sockaddr

import (
	"fmt"
	"strings"
)

type UnixSock struct {
	SockAddr
	path string
}
type UnixSocks []*UnixSock

var unixAttrMap map[AttrName]func(UnixSock) string
var unixAttrs []AttrName

func init() {
	unixAttrInit()
}

func NewUnixSock(s string) (ret UnixSock, err error) {
	ret.path = s
	return ret, nil
}

//
func (us UnixSock) CmpAddress(sa SockAddr) int {
	usb, ok := sa.(UnixSock)
	if !ok {
		return sortDeferDecision
	}

	return strings.Compare(us.Path(), usb.Path())
}

func (us UnixSock) DialPacketArgs() (network, dialArgs string) {
	return "unixgram", us.path
}

func (us UnixSock) DialStreamArgs() (network, dialArgs string) {
	return "unix", us.path
}

func (us UnixSock) Equal(sa SockAddr) bool {
	usb, ok := sa.(UnixSock)
	if !ok {
		return false
	}

	if us.Path() != usb.Path() {
		return false
	}

	return true
}

func (us UnixSock) ListenPacketArgs() (network, dialArgs string) {
	return "unixgram", us.path
}

func (us UnixSock) ListenStreamArgs() (network, dialArgs string) {
	return "unix", us.path
}

func MustUnixSock(addr string) UnixSock {
	us, err := NewUnixSock(addr)
	if err != nil {
		panic(fmt.Sprintf("Unable to create a UnixSock from %+q: %v", addr, err))
	}
	return us
}

func (us UnixSock) Path() string {
	return us.path
}

func (us UnixSock) String() string {
	return fmt.Sprintf("%+q", us.path)
}

func (UnixSock) Type() SockAddrType {
	return TypeUnix
}

func UnixSockAttrs() []AttrName {
	return unixAttrs
}

func UnixSockAttr(us UnixSock, attrName AttrName) string {
	fn, found := unixAttrMap[attrName]
	if !found {
		return ""
	}

	return fn(us)
}

func unixAttrInit() {

	unixAttrs = []AttrName{
		"path",
	}

	unixAttrMap = map[AttrName]func(us UnixSock) string{
		"path": func(us UnixSock) string {
			return us.Path()
		},
	}
}
