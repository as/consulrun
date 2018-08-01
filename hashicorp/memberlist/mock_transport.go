package memberlist

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

type MockNetwork struct {
	transports map[string]*MockTransport
	port       int
}

func (n *MockNetwork) NewTransport() *MockTransport {
	n.port += 1
	addr := fmt.Sprintf("127.0.0.1:%d", n.port)
	transport := &MockTransport{
		net:      n,
		addr:     &MockAddress{addr},
		packetCh: make(chan *Packet),
		streamCh: make(chan net.Conn),
	}

	if n.transports == nil {
		n.transports = make(map[string]*MockTransport)
	}
	n.transports[addr] = transport
	return transport
}

type MockAddress struct {
	addr string
}

func (a *MockAddress) Network() string {
	return "mock"
}

func (a *MockAddress) String() string {
	return a.addr
}

type MockTransport struct {
	net      *MockNetwork
	addr     *MockAddress
	packetCh chan *Packet
	streamCh chan net.Conn
}

func (t *MockTransport) FinalAdvertiseAddr(string, int) (net.IP, int, error) {
	host, portStr, err := net.SplitHostPort(t.addr.String())
	if err != nil {
		return nil, 0, err
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return nil, 0, fmt.Errorf("Failed to parse IP %q", host)
	}

	port, err := strconv.ParseInt(portStr, 10, 16)
	if err != nil {
		return nil, 0, err
	}

	return ip, int(port), nil
}

func (t *MockTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	dest, ok := t.net.transports[addr]
	if !ok {
		return time.Time{}, fmt.Errorf("No route to %q", addr)
	}

	now := time.Now()
	dest.packetCh <- &Packet{
		Buf:       b,
		From:      t.addr,
		Timestamp: now,
	}
	return now, nil
}

func (t *MockTransport) PacketCh() <-chan *Packet {
	return t.packetCh
}

func (t *MockTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	dest, ok := t.net.transports[addr]
	if !ok {
		return nil, fmt.Errorf("No route to %q", addr)
	}

	p1, p2 := net.Pipe()
	dest.streamCh <- p1
	return p2, nil
}

func (t *MockTransport) StreamCh() <-chan net.Conn {
	return t.streamCh
}

func (t *MockTransport) Shutdown() error {
	return nil
}
