package dns

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"time"
)

const dnsTimeout time.Duration = 2 * time.Second
const tcpIdleTimeout time.Duration = 8 * time.Second

type Conn struct {
	net.Conn                         // a net.Conn holding the connection
	UDPSize        uint16            // minimum receive buffer for UDP messages
	TsigSecret     map[string]string // secret(s) for Tsig map[<zonename>]<base64 secret>, zonename must be in canonical form (lowercase, fqdn, see RFC 4034 Section 6.2)
	rtt            time.Duration
	t              time.Time
	tsigRequestMAC string
}

type Client struct {
	Net       string      // if "tcp" or "tcp-tls" (DNS over TLS) a TCP query will be initiated, otherwise an UDP one (default is "" for UDP)
	UDPSize   uint16      // minimum receive buffer for UDP messages
	TLSConfig *tls.Config // TLS connection configuration
	Dialer    *net.Dialer // a net.Dialer used to set local address, timeouts and more

	Timeout        time.Duration
	DialTimeout    time.Duration     // net.DialTimeout, defaults to 2 seconds, or net.Dialer.Timeout if expiring earlier - overridden by Timeout when that value is non-zero
	ReadTimeout    time.Duration     // net.Conn.SetReadTimeout value for connections, defaults to 2 seconds - overridden by Timeout when that value is non-zero
	WriteTimeout   time.Duration     // net.Conn.SetWriteTimeout value for connections, defaults to 2 seconds - overridden by Timeout when that value is non-zero
	TsigSecret     map[string]string // secret(s) for Tsig map[<zonename>]<base64 secret>, zonename must be in canonical form (lowercase, fqdn, see RFC 4034 Section 6.2)
	SingleInflight bool              // if true suppress multiple outstanding queries for the same Qname, Qtype and Qclass
	group          singleflight
}

func Exchange(m *Msg, a string) (r *Msg, err error) {
	client := Client{Net: "udp"}
	r, _, err = client.Exchange(m, a)
	return r, err
}

func (c *Client) dialTimeout() time.Duration {
	if c.Timeout != 0 {
		return c.Timeout
	}
	if c.DialTimeout != 0 {
		return c.DialTimeout
	}
	return dnsTimeout
}

func (c *Client) readTimeout() time.Duration {
	if c.ReadTimeout != 0 {
		return c.ReadTimeout
	}
	return dnsTimeout
}

func (c *Client) writeTimeout() time.Duration {
	if c.WriteTimeout != 0 {
		return c.WriteTimeout
	}
	return dnsTimeout
}

func (c *Client) Dial(address string) (conn *Conn, err error) {

	var d net.Dialer
	if c.Dialer == nil {
		d = net.Dialer{}
	} else {
		d = net.Dialer(*c.Dialer)
	}
	d.Timeout = c.getTimeoutForRequest(c.writeTimeout())

	network := "udp"
	useTLS := false

	switch c.Net {
	case "tcp-tls":
		network = "tcp"
		useTLS = true
	case "tcp4-tls":
		network = "tcp4"
		useTLS = true
	case "tcp6-tls":
		network = "tcp6"
		useTLS = true
	default:
		if c.Net != "" {
			network = c.Net
		}
	}

	conn = new(Conn)
	if useTLS {
		conn.Conn, err = tls.DialWithDialer(&d, network, address, c.TLSConfig)
	} else {
		conn.Conn, err = d.Dial(network, address)
	}
	if err != nil {
		return nil, err
	}
	return conn, nil
}

//
//
func (c *Client) Exchange(m *Msg, address string) (r *Msg, rtt time.Duration, err error) {
	if !c.SingleInflight {
		return c.exchange(m, address)
	}

	t := "nop"
	if t1, ok := TypeToString[m.Question[0].Qtype]; ok {
		t = t1
	}
	cl := "nop"
	if cl1, ok := ClassToString[m.Question[0].Qclass]; ok {
		cl = cl1
	}
	r, rtt, err, shared := c.group.Do(m.Question[0].Name+t+cl, func() (*Msg, time.Duration, error) {
		return c.exchange(m, address)
	})
	if r != nil && shared {
		r = r.Copy()
	}
	return r, rtt, err
}

func (c *Client) exchange(m *Msg, a string) (r *Msg, rtt time.Duration, err error) {
	var co *Conn

	co, err = c.Dial(a)

	if err != nil {
		return nil, 0, err
	}
	defer co.Close()

	opt := m.IsEdns0()

	if opt != nil && opt.UDPSize() >= MinMsgSize {
		co.UDPSize = opt.UDPSize()
	}

	if opt == nil && c.UDPSize >= MinMsgSize {
		co.UDPSize = c.UDPSize
	}

	co.TsigSecret = c.TsigSecret

	co.SetWriteDeadline(time.Now().Add(c.getTimeoutForRequest(c.writeTimeout())))
	if err = co.WriteMsg(m); err != nil {
		return nil, 0, err
	}

	co.SetReadDeadline(time.Now().Add(c.getTimeoutForRequest(c.readTimeout())))
	r, err = co.ReadMsg()
	if err == nil && r.Id != m.Id {
		err = ErrId
	}
	return r, co.rtt, err
}

func (co *Conn) ReadMsg() (*Msg, error) {
	p, err := co.ReadMsgHeader(nil)
	if err != nil {
		return nil, err
	}

	m := new(Msg)
	if err := m.Unpack(p); err != nil {

		return m, err
	}
	if t := m.IsTsig(); t != nil {
		if _, ok := co.TsigSecret[t.Hdr.Name]; !ok {
			return m, ErrSecret
		}

		err = TsigVerify(p, co.TsigSecret[t.Hdr.Name], co.tsigRequestMAC, false)
	}
	return m, err
}

func (co *Conn) ReadMsgHeader(hdr *Header) ([]byte, error) {
	var (
		p   []byte
		n   int
		err error
	)

	switch t := co.Conn.(type) {
	case *net.TCPConn, *tls.Conn:
		r := t.(io.Reader)

		l, err := tcpMsgLen(r)
		if err != nil {
			return nil, err
		}
		p = make([]byte, l)
		n, err = tcpRead(r, p)
		co.rtt = time.Since(co.t)
	default:
		if co.UDPSize > MinMsgSize {
			p = make([]byte, co.UDPSize)
		} else {
			p = make([]byte, MinMsgSize)
		}
		n, err = co.Read(p)
		co.rtt = time.Since(co.t)
	}

	if err != nil {
		return nil, err
	} else if n < headerSize {
		return nil, ErrShortRead
	}

	p = p[:n]
	if hdr != nil {
		dh, _, err := unpackMsgHdr(p, 0)
		if err != nil {
			return nil, err
		}
		*hdr = dh
	}
	return p, err
}

func tcpMsgLen(t io.Reader) (int, error) {
	p := []byte{0, 0}
	n, err := t.Read(p)
	if err != nil {
		return 0, err
	}

	if n == 1 {
		n1, err := t.Read(p[1:])
		if err != nil {
			return 0, err
		}
		n += n1
	}

	if n != 2 {
		return 0, ErrShortRead
	}
	l := binary.BigEndian.Uint16(p)
	if l == 0 {
		return 0, ErrShortRead
	}
	return int(l), nil
}

func tcpRead(t io.Reader, p []byte) (int, error) {
	n, err := t.Read(p)
	if err != nil {
		return n, err
	}
	for n < len(p) {
		j, err := t.Read(p[n:])
		if err != nil {
			return n, err
		}
		n += j
	}
	return n, err
}

func (co *Conn) Read(p []byte) (n int, err error) {
	if co.Conn == nil {
		return 0, ErrConnEmpty
	}
	if len(p) < 2 {
		return 0, io.ErrShortBuffer
	}
	switch t := co.Conn.(type) {
	case *net.TCPConn, *tls.Conn:
		r := t.(io.Reader)

		l, err := tcpMsgLen(r)
		if err != nil {
			return 0, err
		}
		if l > len(p) {
			return int(l), io.ErrShortBuffer
		}
		return tcpRead(r, p[:l])
	}

	n, err = co.Conn.Read(p)
	if err != nil {
		return n, err
	}
	return n, err
}

func (co *Conn) WriteMsg(m *Msg) (err error) {
	var out []byte
	if t := m.IsTsig(); t != nil {
		mac := ""
		if _, ok := co.TsigSecret[t.Hdr.Name]; !ok {
			return ErrSecret
		}
		out, mac, err = TsigGenerate(m, co.TsigSecret[t.Hdr.Name], co.tsigRequestMAC, false)

		co.tsigRequestMAC = mac
	} else {
		out, err = m.Pack()
	}
	if err != nil {
		return err
	}
	co.t = time.Now()
	if _, err = co.Write(out); err != nil {
		return err
	}
	return nil
}

func (co *Conn) Write(p []byte) (n int, err error) {
	switch t := co.Conn.(type) {
	case *net.TCPConn, *tls.Conn:
		w := t.(io.Writer)

		lp := len(p)
		if lp < 2 {
			return 0, io.ErrShortBuffer
		}
		if lp > MaxMsgSize {
			return 0, &Error{err: "message too large"}
		}
		l := make([]byte, 2, lp+2)
		binary.BigEndian.PutUint16(l, uint16(lp))
		p = append(l, p...)
		n, err := io.Copy(w, bytes.NewReader(p))
		return int(n), err
	}
	n, err = co.Conn.Write(p)
	return n, err
}

func (c *Client) getTimeoutForRequest(timeout time.Duration) time.Duration {
	var requestTimeout time.Duration
	if c.Timeout != 0 {
		requestTimeout = c.Timeout
	} else {
		requestTimeout = timeout
	}

	if c.Dialer != nil && c.Dialer.Timeout != 0 {
		if c.Dialer.Timeout < requestTimeout {
			requestTimeout = c.Dialer.Timeout
		}
	}
	return requestTimeout
}

func Dial(network, address string) (conn *Conn, err error) {
	conn = new(Conn)
	conn.Conn, err = net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func ExchangeContext(ctx context.Context, m *Msg, a string) (r *Msg, err error) {
	client := Client{Net: "udp"}
	r, _, err = client.ExchangeContext(ctx, m, a)

	return r, err
}

//
//
func ExchangeConn(c net.Conn, m *Msg) (r *Msg, err error) {
	println("dns: ExchangeConn: this function is deprecated")
	co := new(Conn)
	co.Conn = c
	if err = co.WriteMsg(m); err != nil {
		return nil, err
	}
	r, err = co.ReadMsg()
	if err == nil && r.Id != m.Id {
		err = ErrId
	}
	return r, err
}

func DialTimeout(network, address string, timeout time.Duration) (conn *Conn, err error) {
	client := Client{Net: network, Dialer: &net.Dialer{Timeout: timeout}}
	conn, err = client.Dial(address)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func DialWithTLS(network, address string, tlsConfig *tls.Config) (conn *Conn, err error) {
	if !strings.HasSuffix(network, "-tls") {
		network += "-tls"
	}
	client := Client{Net: network, TLSConfig: tlsConfig}
	conn, err = client.Dial(address)

	if err != nil {
		return nil, err
	}
	return conn, nil
}

func DialTimeoutWithTLS(network, address string, tlsConfig *tls.Config, timeout time.Duration) (conn *Conn, err error) {
	if !strings.HasSuffix(network, "-tls") {
		network += "-tls"
	}
	client := Client{Net: network, Dialer: &net.Dialer{Timeout: timeout}, TLSConfig: tlsConfig}
	conn, err = client.Dial(address)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *Client) ExchangeContext(ctx context.Context, m *Msg, a string) (r *Msg, rtt time.Duration, err error) {
	var timeout time.Duration
	if deadline, ok := ctx.Deadline(); !ok {
		timeout = 0
	} else {
		timeout = deadline.Sub(time.Now())
	}

	c.Dialer = &net.Dialer{Timeout: timeout}
	return c.Exchange(m, a)
}
