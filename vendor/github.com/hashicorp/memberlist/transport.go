package memberlist

import (
	"net"
	"time"
)

type Packet struct {
	Buf []byte

	From net.Addr

	Timestamp time.Time
}

type Transport interface {
	FinalAdvertiseAddr(ip string, port int) (net.IP, int, error)

	//

	WriteTo(b []byte, addr string) (time.Time, error)

	PacketCh() <-chan *Packet

	DialTimeout(addr string, timeout time.Duration) (net.Conn, error)

	StreamCh() <-chan net.Conn

	Shutdown() error
}
