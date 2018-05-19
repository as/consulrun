package memberlist

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/armon/go-metrics"
	sockaddr "github.com/hashicorp/go-sockaddr"
)

const (
	udpPacketBufSize = 65536

	udpRecvBufSize = 2 * 1024 * 1024
)

type NetTransportConfig struct {
	BindAddrs []string

	BindPort int

	Logger *log.Logger
}

type NetTransport struct {
	config       *NetTransportConfig
	packetCh     chan *Packet
	streamCh     chan net.Conn
	logger       *log.Logger
	wg           sync.WaitGroup
	tcpListeners []*net.TCPListener
	udpListeners []*net.UDPConn
	shutdown     int32
}

func NewNetTransport(config *NetTransportConfig) (*NetTransport, error) {

	if len(config.BindAddrs) == 0 {
		return nil, fmt.Errorf("At least one bind address is required")
	}

	var ok bool
	t := NetTransport{
		config:   config,
		packetCh: make(chan *Packet),
		streamCh: make(chan net.Conn),
		logger:   config.Logger,
	}

	defer func() {
		if !ok {
			t.Shutdown()
		}
	}()

	port := config.BindPort
	for _, addr := range config.BindAddrs {
		ip := net.ParseIP(addr)

		tcpAddr := &net.TCPAddr{IP: ip, Port: port}
		tcpLn, err := net.ListenTCP("tcp", tcpAddr)
		if err != nil {
			return nil, fmt.Errorf("Failed to start TCP listener on %q port %d: %v", addr, port, err)
		}
		t.tcpListeners = append(t.tcpListeners, tcpLn)

		if port == 0 {
			port = tcpLn.Addr().(*net.TCPAddr).Port
		}

		udpAddr := &net.UDPAddr{IP: ip, Port: port}
		udpLn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			return nil, fmt.Errorf("Failed to start UDP listener on %q port %d: %v", addr, port, err)
		}
		if err := setUDPRecvBuf(udpLn); err != nil {
			return nil, fmt.Errorf("Failed to resize UDP buffer: %v", err)
		}
		t.udpListeners = append(t.udpListeners, udpLn)
	}

	for i := 0; i < len(config.BindAddrs); i++ {
		t.wg.Add(2)
		go t.tcpListen(t.tcpListeners[i])
		go t.udpListen(t.udpListeners[i])
	}

	ok = true
	return &t, nil
}

func (t *NetTransport) GetAutoBindPort() int {

	return t.tcpListeners[0].Addr().(*net.TCPAddr).Port
}

func (t *NetTransport) FinalAdvertiseAddr(ip string, port int) (net.IP, int, error) {
	var advertiseAddr net.IP
	var advertisePort int
	if ip != "" {

		advertiseAddr = net.ParseIP(ip)
		if advertiseAddr == nil {
			return nil, 0, fmt.Errorf("Failed to parse advertise address %q", ip)
		}

		if ip4 := advertiseAddr.To4(); ip4 != nil {
			advertiseAddr = ip4
		}
		advertisePort = port
	} else {
		if t.config.BindAddrs[0] == "0.0.0.0" {

			var err error
			ip, err = sockaddr.GetPrivateIP()
			if err != nil {
				return nil, 0, fmt.Errorf("Failed to get interface addresses: %v", err)
			}
			if ip == "" {
				return nil, 0, fmt.Errorf("No private IP address found, and explicit IP not provided")
			}

			advertiseAddr = net.ParseIP(ip)
			if advertiseAddr == nil {
				return nil, 0, fmt.Errorf("Failed to parse advertise address: %q", ip)
			}
		} else {

			advertiseAddr = t.tcpListeners[0].Addr().(*net.TCPAddr).IP
		}

		advertisePort = t.GetAutoBindPort()
	}

	return advertiseAddr, advertisePort, nil
}

func (t *NetTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return time.Time{}, err
	}

	_, err = t.udpListeners[0].WriteTo(b, udpAddr)
	return time.Now(), err
}

func (t *NetTransport) PacketCh() <-chan *Packet {
	return t.packetCh
}

func (t *NetTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	return dialer.Dial("tcp", addr)
}

func (t *NetTransport) StreamCh() <-chan net.Conn {
	return t.streamCh
}

func (t *NetTransport) Shutdown() error {

	atomic.StoreInt32(&t.shutdown, 1)

	for _, conn := range t.tcpListeners {
		conn.Close()
	}
	for _, conn := range t.udpListeners {
		conn.Close()
	}

	t.wg.Wait()
	return nil
}

func (t *NetTransport) tcpListen(tcpLn *net.TCPListener) {
	defer t.wg.Done()
	for {
		conn, err := tcpLn.AcceptTCP()
		if err != nil {
			if s := atomic.LoadInt32(&t.shutdown); s == 1 {
				break
			}

			t.logger.Printf("[ERR] memberlist: Error accepting TCP connection: %v", err)
			continue
		}

		t.streamCh <- conn
	}
}

func (t *NetTransport) udpListen(udpLn *net.UDPConn) {
	defer t.wg.Done()
	for {

		buf := make([]byte, udpPacketBufSize)
		n, addr, err := udpLn.ReadFrom(buf)
		ts := time.Now()
		if err != nil {
			if s := atomic.LoadInt32(&t.shutdown); s == 1 {
				break
			}

			t.logger.Printf("[ERR] memberlist: Error reading UDP packet: %v", err)
			continue
		}

		if n < 1 {
			t.logger.Printf("[ERR] memberlist: UDP packet too short (%d bytes) %s",
				len(buf), LogAddress(addr))
			continue
		}

		metrics.IncrCounter([]string{"memberlist", "udp", "received"}, float32(n))
		t.packetCh <- &Packet{
			Buf:       buf[:n],
			From:      addr,
			Timestamp: ts,
		}
	}
}

func setUDPRecvBuf(c *net.UDPConn) error {
	size := udpRecvBufSize
	var err error
	for size > 0 {
		if err = c.SetReadBuffer(size); err == nil {
			return nil
		}
		size = size / 2
	}
	return err
}
