package pool

import (
	"container/list"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/lib"
	"github.com/hashicorp/consul/tlsutil"
	"github.com/hashicorp/net-rpc-msgpackrpc"
	"github.com/hashicorp/yamux"
)

const defaultDialTimeout = 10 * time.Second

type muxSession interface {
	Open() (net.Conn, error)
	Close() error
}

type StreamClient struct {
	stream net.Conn
	codec  rpc.ClientCodec
}

func (sc *StreamClient) Close() {
	sc.stream.Close()
	sc.codec.Close()
}

type Conn struct {
	refCount    int32
	shouldClose int32

	addr     net.Addr
	session  muxSession
	lastUsed time.Time
	version  int

	pool *ConnPool

	clients    *list.List
	clientLock sync.Mutex
}

func (c *Conn) Close() error {
	return c.session.Close()
}

func (c *Conn) getClient() (*StreamClient, error) {

	c.clientLock.Lock()
	front := c.clients.Front()
	if front != nil {
		c.clients.Remove(front)
	}
	c.clientLock.Unlock()
	if front != nil {
		return front.Value.(*StreamClient), nil
	}

	stream, err := c.session.Open()
	if err != nil {
		return nil, err
	}

	codec := msgpackrpc.NewClientCodec(stream)

	sc := &StreamClient{
		stream: stream,
		codec:  codec,
	}
	return sc, nil
}

func (c *Conn) returnClient(client *StreamClient) {
	didSave := false
	c.clientLock.Lock()
	if c.clients.Len() < c.pool.MaxStreams && atomic.LoadInt32(&c.shouldClose) == 0 {
		c.clients.PushFront(client)
		didSave = true

		if ys, ok := client.stream.(*yamux.Stream); ok {
			ys.Shrink()
		}
	}
	c.clientLock.Unlock()
	if !didSave {
		client.Close()
	}
}

func (c *Conn) markForUse() {
	c.lastUsed = time.Now()
	atomic.AddInt32(&c.refCount, 1)
}

type ConnPool struct {
	SrcAddr *net.TCPAddr

	LogOutput io.Writer

	MaxTime time.Duration

	MaxStreams int

	TLSWrapper tlsutil.DCWrapper

	ForceTLS bool

	sync.Mutex

	pool map[string]*Conn

	limiter map[string]chan struct{}

	shutdown   bool
	shutdownCh chan struct{}

	once sync.Once
}

func (p *ConnPool) init() {
	p.pool = make(map[string]*Conn)
	p.limiter = make(map[string]chan struct{})
	p.shutdownCh = make(chan struct{})
	if p.MaxTime > 0 {
		go p.reap()
	}
}

func (p *ConnPool) Shutdown() error {
	p.once.Do(p.init)

	p.Lock()
	defer p.Unlock()

	for _, conn := range p.pool {
		conn.Close()
	}
	p.pool = make(map[string]*Conn)

	if p.shutdown {
		return nil
	}
	p.shutdown = true
	close(p.shutdownCh)
	return nil
}

func (p *ConnPool) acquire(dc string, addr net.Addr, version int, useTLS bool) (*Conn, error) {
	addrStr := addr.String()

	p.Lock()
	c := p.pool[addrStr]
	if c != nil {
		c.markForUse()
		p.Unlock()
		return c, nil
	}

	var wait chan struct{}
	var ok bool
	if wait, ok = p.limiter[addrStr]; !ok {
		wait = make(chan struct{})
		p.limiter[addrStr] = wait
	}
	isLeadThread := !ok
	p.Unlock()

	if isLeadThread {
		c, err := p.getNewConn(dc, addr, version, useTLS)
		p.Lock()
		delete(p.limiter, addrStr)
		close(wait)
		if err != nil {
			p.Unlock()
			return nil, err
		}

		p.pool[addrStr] = c
		p.Unlock()
		return c, nil
	}

	select {
	case <-p.shutdownCh:
		return nil, fmt.Errorf("rpc error: shutdown")
	case <-wait:
	}

	p.Lock()
	if c := p.pool[addrStr]; c != nil {
		c.markForUse()
		p.Unlock()
		return c, nil
	}

	p.Unlock()
	return nil, fmt.Errorf("rpc error: lead thread didn't get connection")
}

type HalfCloser interface {
	CloseWrite() error
}

func (p *ConnPool) DialTimeout(dc string, addr net.Addr, timeout time.Duration, useTLS bool) (net.Conn, HalfCloser, error) {
	p.once.Do(p.init)

	d := &net.Dialer{LocalAddr: p.SrcAddr, Timeout: timeout}
	conn, err := d.Dial("tcp", addr.String())
	if err != nil {
		return nil, nil, err
	}

	var hc HalfCloser
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetKeepAlive(true)
		tcp.SetNoDelay(true)
		hc = tcp
	}

	if (useTLS || p.ForceTLS) && p.TLSWrapper != nil {

		if _, err := conn.Write([]byte{byte(RPCTLS)}); err != nil {
			conn.Close()
			return nil, nil, err
		}

		tlsConn, err := p.TLSWrapper(dc, conn)
		if err != nil {
			conn.Close()
			return nil, nil, err
		}
		conn = tlsConn
	}

	return conn, hc, nil
}

func (p *ConnPool) getNewConn(dc string, addr net.Addr, version int, useTLS bool) (*Conn, error) {

	conn, _, err := p.DialTimeout(dc, addr, defaultDialTimeout, useTLS)
	if err != nil {
		return nil, err
	}

	var session muxSession
	if version < 2 {
		conn.Close()
		return nil, fmt.Errorf("cannot make client connection, unsupported protocol version %d", version)
	}

	if _, err := conn.Write([]byte{byte(RPCMultiplexV2)}); err != nil {
		conn.Close()
		return nil, err
	}

	conf := yamux.DefaultConfig()
	conf.LogOutput = p.LogOutput

	session, _ = yamux.Client(conn, conf)

	c := &Conn{
		refCount: 1,
		addr:     addr,
		session:  session,
		clients:  list.New(),
		lastUsed: time.Now(),
		version:  version,
		pool:     p,
	}
	return c, nil
}

func (p *ConnPool) clearConn(conn *Conn) {

	atomic.StoreInt32(&conn.shouldClose, 1)

	addrStr := conn.addr.String()
	p.Lock()
	if c, ok := p.pool[addrStr]; ok && c == conn {
		delete(p.pool, addrStr)
	}
	p.Unlock()

	if refCount := atomic.LoadInt32(&conn.refCount); refCount == 0 {
		conn.Close()
	}
}

func (p *ConnPool) releaseConn(conn *Conn) {
	refCount := atomic.AddInt32(&conn.refCount, -1)
	if refCount == 0 && atomic.LoadInt32(&conn.shouldClose) == 1 {
		conn.Close()
	}
}

func (p *ConnPool) getClient(dc string, addr net.Addr, version int, useTLS bool) (*Conn, *StreamClient, error) {
	retries := 0
START:

	conn, err := p.acquire(dc, addr, version, useTLS)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get conn: %v", err)
	}

	client, err := conn.getClient()
	if err != nil {
		p.clearConn(conn)
		p.releaseConn(conn)

		if retries == 0 {
			retries++
			goto START
		}
		return nil, nil, fmt.Errorf("failed to start stream: %v", err)
	}
	return conn, client, nil
}

func (p *ConnPool) RPC(dc string, addr net.Addr, version int, method string, useTLS bool, args interface{}, reply interface{}) error {
	p.once.Do(p.init)

	conn, sc, err := p.getClient(dc, addr, version, useTLS)
	if err != nil {
		return fmt.Errorf("rpc error getting client: %v", err)
	}

	err = msgpackrpc.CallWithCodec(sc.codec, method, args, reply)
	if err != nil {
		sc.Close()

		if lib.IsErrEOF(err) {
			p.clearConn(conn)
		}

		p.releaseConn(conn)
		return fmt.Errorf("rpc error making call: %v", err)
	}

	conn.returnClient(sc)
	p.releaseConn(conn)
	return nil
}

func (p *ConnPool) Ping(dc string, addr net.Addr, version int, useTLS bool) (bool, error) {
	var out struct{}
	err := p.RPC(dc, addr, version, "Status.Ping", useTLS, struct{}{}, &out)
	return err == nil, err
}

func (p *ConnPool) reap() {
	for {

		select {
		case <-p.shutdownCh:
			return
		case <-time.After(time.Second):
		}

		p.Lock()
		var removed []string
		now := time.Now()
		for host, conn := range p.pool {

			if now.Sub(conn.lastUsed) < p.MaxTime {
				continue
			}

			if atomic.LoadInt32(&conn.refCount) > 0 {
				continue
			}

			conn.Close()

			removed = append(removed, host)
		}
		for _, host := range removed {
			delete(p.pool, host)
		}
		p.Unlock()
	}
}
