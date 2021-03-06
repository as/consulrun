package raft

import (
	"errors"
	"io"
	"log"
	"net"
	"time"
)

var (
	errNotAdvertisable = errors.New("local bind address is not advertisable")
	errNotTCP          = errors.New("local address is not a TCP address")
)

type TCPStreamLayer struct {
	advertise net.Addr
	listener  *net.TCPListener
}

func NewTCPTransport(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		return NewNetworkTransport(stream, maxPool, timeout, logOutput)
	})
}

func NewTCPTransportWithLogger(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logger *log.Logger,
) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		return NewNetworkTransportWithLogger(stream, maxPool, timeout, logger)
	})
}

func NewTCPTransportWithConfig(
	bindAddr string,
	advertise net.Addr,
	config *NetworkTransportConfig,
) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		config.Stream = stream
		return NewNetworkTransportWithConfig(config)
	})
}

func newTCPTransport(bindAddr string,
	advertise net.Addr,
	transportCreator func(stream StreamLayer) *NetworkTransport) (*NetworkTransport, error) {

	list, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	stream := &TCPStreamLayer{
		advertise: advertise,
		listener:  list.(*net.TCPListener),
	}

	addr, ok := stream.Addr().(*net.TCPAddr)
	if !ok {
		list.Close()
		return nil, errNotTCP
	}
	if addr.IP.IsUnspecified() {
		list.Close()
		return nil, errNotAdvertisable
	}

	trans := transportCreator(stream)
	return trans, nil
}

func (t *TCPStreamLayer) Dial(address ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

func (t *TCPStreamLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

func (t *TCPStreamLayer) Close() (err error) {
	return t.listener.Close()
}

func (t *TCPStreamLayer) Addr() net.Addr {

	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}
