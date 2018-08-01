package yamux

import (
	"fmt"
	"net"
)

type hasAddr interface {
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

type yamuxAddr struct {
	Addr string
}

func (*yamuxAddr) Network() string {
	return "yamux"
}

func (y *yamuxAddr) String() string {
	return fmt.Sprintf("yamux:%s", y.Addr)
}

func (s *Session) Addr() net.Addr {
	return s.LocalAddr()
}

func (s *Session) LocalAddr() net.Addr {
	addr, ok := s.conn.(hasAddr)
	if !ok {
		return &yamuxAddr{"local"}
	}
	return addr.LocalAddr()
}

func (s *Session) RemoteAddr() net.Addr {
	addr, ok := s.conn.(hasAddr)
	if !ok {
		return &yamuxAddr{"remote"}
	}
	return addr.RemoteAddr()
}

func (s *Stream) LocalAddr() net.Addr {
	return s.session.LocalAddr()
}

func (s *Stream) RemoteAddr() net.Addr {
	return s.session.RemoteAddr()
}
