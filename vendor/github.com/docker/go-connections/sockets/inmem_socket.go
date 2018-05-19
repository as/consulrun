package sockets

import (
	"errors"
	"net"
	"sync"
)

var errClosed = errors.New("use of closed network connection")

type InmemSocket struct {
	chConn  chan net.Conn
	chClose chan struct{}
	addr    string
	mu      sync.Mutex
}

type dummyAddr string

func NewInmemSocket(addr string, bufSize int) *InmemSocket {
	return &InmemSocket{
		chConn:  make(chan net.Conn, bufSize),
		chClose: make(chan struct{}),
		addr:    addr,
	}
}

func (s *InmemSocket) Addr() net.Addr {
	return dummyAddr(s.addr)
}

func (s *InmemSocket) Accept() (net.Conn, error) {
	select {
	case conn := <-s.chConn:
		return conn, nil
	case <-s.chClose:
		return nil, errClosed
	}
}

func (s *InmemSocket) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.chClose:
	default:
		close(s.chClose)
	}
	return nil
}

func (s *InmemSocket) Dial(network, addr string) (net.Conn, error) {
	srvConn, clientConn := net.Pipe()
	select {
	case s.chConn <- srvConn:
	case <-s.chClose:
		return nil, errClosed
	}

	return clientConn, nil
}

func (a dummyAddr) Network() string {
	return string(a)
}

func (a dummyAddr) String() string {
	return string(a)
}
