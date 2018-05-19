package consul

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/consul/agent/pool"
	"github.com/hashicorp/consul/tlsutil"
	"github.com/hashicorp/raft"
)

type RaftLayer struct {
	src net.Addr

	addr net.Addr

	connCh chan net.Conn

	tlsWrap tlsutil.Wrapper

	closed    bool
	closeCh   chan struct{}
	closeLock sync.Mutex

	tlsFunc func(raft.ServerAddress) bool
}

func NewRaftLayer(src, addr net.Addr, tlsWrap tlsutil.Wrapper, tlsFunc func(raft.ServerAddress) bool) *RaftLayer {
	layer := &RaftLayer{
		src:     src,
		addr:    addr,
		connCh:  make(chan net.Conn),
		tlsWrap: tlsWrap,
		closeCh: make(chan struct{}),
		tlsFunc: tlsFunc,
	}
	return layer
}

func (l *RaftLayer) Handoff(c net.Conn) error {
	select {
	case l.connCh <- c:
		return nil
	case <-l.closeCh:
		return fmt.Errorf("Raft RPC layer closed")
	}
}

func (l *RaftLayer) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connCh:
		return conn, nil
	case <-l.closeCh:
		return nil, fmt.Errorf("Raft RPC layer closed")
	}
}

func (l *RaftLayer) Close() error {
	l.closeLock.Lock()
	defer l.closeLock.Unlock()

	if !l.closed {
		l.closed = true
		close(l.closeCh)
	}
	return nil
}

func (l *RaftLayer) Addr() net.Addr {
	return l.addr
}

func (l *RaftLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := &net.Dialer{LocalAddr: l.src, Timeout: timeout}
	conn, err := d.Dial("tcp", string(address))
	if err != nil {
		return nil, err
	}

	if l.tlsFunc(address) && l.tlsWrap != nil {

		if _, err := conn.Write([]byte{byte(pool.RPCTLS)}); err != nil {
			conn.Close()
			return nil, err
		}

		conn, err = l.tlsWrap(conn)
		if err != nil {
			return nil, err
		}
	}

	_, err = conn.Write([]byte{byte(pool.RPCRaft)})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, err
}
