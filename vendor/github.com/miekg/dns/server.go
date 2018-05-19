// DNS server implementation.

package dns

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"
)

const maxTCPQueries = 128

type Handler interface {
	ServeDNS(w ResponseWriter, r *Msg)
}

type ResponseWriter interface {
	LocalAddr() net.Addr

	RemoteAddr() net.Addr

	WriteMsg(*Msg) error

	Write([]byte) (int, error)

	Close() error

	TsigStatus() error

	TsigTimersOnly(bool)

	Hijack()
}

type response struct {
	hijacked       bool // connection has been hijacked by handler
	tsigStatus     error
	tsigTimersOnly bool
	tsigRequestMAC string
	tsigSecret     map[string]string // the tsig secrets
	udp            *net.UDPConn      // i/o connection if UDP was used
	tcp            net.Conn          // i/o connection if TCP was used
	udpSession     *SessionUDP       // oob data to get egress interface right
	remoteAddr     net.Addr          // address of the client
	writer         Writer            // writer to output the raw DNS bits
}

type ServeMux struct {
	z map[string]Handler
	m *sync.RWMutex
}

func NewServeMux() *ServeMux { return &ServeMux{z: make(map[string]Handler), m: new(sync.RWMutex)} }

var DefaultServeMux = NewServeMux()

type HandlerFunc func(ResponseWriter, *Msg)

func (f HandlerFunc) ServeDNS(w ResponseWriter, r *Msg) {
	f(w, r)
}

func HandleFailed(w ResponseWriter, r *Msg) {
	m := new(Msg)
	m.SetRcode(r, RcodeServerFailure)

	w.WriteMsg(m)
}

func failedHandler() Handler { return HandlerFunc(HandleFailed) }

func ListenAndServe(addr string, network string, handler Handler) error {
	server := &Server{Addr: addr, Net: network, Handler: handler}
	return server.ListenAndServe()
}

func ListenAndServeTLS(addr, certFile, keyFile string, handler Handler) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}

	config := tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	server := &Server{
		Addr:      addr,
		Net:       "tcp-tls",
		TLSConfig: &config,
		Handler:   handler,
	}

	return server.ListenAndServe()
}

func ActivateAndServe(l net.Listener, p net.PacketConn, handler Handler) error {
	server := &Server{Listener: l, PacketConn: p, Handler: handler}
	return server.ActivateAndServe()
}

func (mux *ServeMux) match(q string, t uint16) Handler {
	mux.m.RLock()
	defer mux.m.RUnlock()
	var handler Handler
	b := make([]byte, len(q)) // worst case, one label of length q
	off := 0
	end := false
	for {
		l := len(q[off:])
		for i := 0; i < l; i++ {
			b[i] = q[off+i]
			if b[i] >= 'A' && b[i] <= 'Z' {
				b[i] |= ('a' - 'A')
			}
		}
		if h, ok := mux.z[string(b[:l])]; ok { // causes garbage, might want to change the map key
			if t != TypeDS {
				return h
			}

			handler = h
		}
		off, end = NextLabel(q, off)
		if end {
			break
		}
	}

	if h, ok := mux.z["."]; ok {
		return h
	}
	return handler
}

func (mux *ServeMux) Handle(pattern string, handler Handler) {
	if pattern == "" {
		panic("dns: invalid pattern " + pattern)
	}
	mux.m.Lock()
	mux.z[Fqdn(pattern)] = handler
	mux.m.Unlock()
}

func (mux *ServeMux) HandleFunc(pattern string, handler func(ResponseWriter, *Msg)) {
	mux.Handle(pattern, HandlerFunc(handler))
}

func (mux *ServeMux) HandleRemove(pattern string) {
	if pattern == "" {
		panic("dns: invalid pattern " + pattern)
	}
	mux.m.Lock()
	delete(mux.z, Fqdn(pattern))
	mux.m.Unlock()
}

func (mux *ServeMux) ServeDNS(w ResponseWriter, request *Msg) {
	var h Handler
	if len(request.Question) < 1 { // allow more than one question
		h = failedHandler()
	} else {
		if h = mux.match(request.Question[0].Name, request.Question[0].Qtype); h == nil {
			h = failedHandler()
		}
	}
	h.ServeDNS(w, request)
}

func Handle(pattern string, handler Handler) { DefaultServeMux.Handle(pattern, handler) }

func HandleRemove(pattern string) { DefaultServeMux.HandleRemove(pattern) }

func HandleFunc(pattern string, handler func(ResponseWriter, *Msg)) {
	DefaultServeMux.HandleFunc(pattern, handler)
}

type Writer interface {
	io.Writer
}

type Reader interface {
	ReadTCP(conn net.Conn, timeout time.Duration) ([]byte, error)

	ReadUDP(conn *net.UDPConn, timeout time.Duration) ([]byte, *SessionUDP, error)
}

type defaultReader struct {
	*Server
}

func (dr *defaultReader) ReadTCP(conn net.Conn, timeout time.Duration) ([]byte, error) {
	return dr.readTCP(conn, timeout)
}

func (dr *defaultReader) ReadUDP(conn *net.UDPConn, timeout time.Duration) ([]byte, *SessionUDP, error) {
	return dr.readUDP(conn, timeout)
}

type DecorateReader func(Reader) Reader

type DecorateWriter func(Writer) Writer

type Server struct {
	Addr string

	Net string

	Listener net.Listener

	TLSConfig *tls.Config

	PacketConn net.PacketConn

	Handler Handler

	UDPSize int

	ReadTimeout time.Duration

	WriteTimeout time.Duration

	IdleTimeout func() time.Duration

	TsigSecret map[string]string

	Unsafe bool

	NotifyStartedFunc func()

	DecorateReader DecorateReader

	DecorateWriter DecorateWriter

	lock    sync.RWMutex
	started bool
}

func (srv *Server) ListenAndServe() error {
	srv.lock.Lock()
	defer srv.lock.Unlock()
	if srv.started {
		return &Error{err: "server already started"}
	}
	addr := srv.Addr
	if addr == "" {
		addr = ":domain"
	}
	if srv.UDPSize == 0 {
		srv.UDPSize = MinMsgSize
	}
	switch srv.Net {
	case "tcp", "tcp4", "tcp6":
		a, err := net.ResolveTCPAddr(srv.Net, addr)
		if err != nil {
			return err
		}
		l, err := net.ListenTCP(srv.Net, a)
		if err != nil {
			return err
		}
		srv.Listener = l
		srv.started = true
		srv.lock.Unlock()
		err = srv.serveTCP(l)
		srv.lock.Lock() // to satisfy the defer at the top
		return err
	case "tcp-tls", "tcp4-tls", "tcp6-tls":
		network := "tcp"
		if srv.Net == "tcp4-tls" {
			network = "tcp4"
		} else if srv.Net == "tcp6-tls" {
			network = "tcp6"
		}

		l, err := tls.Listen(network, addr, srv.TLSConfig)
		if err != nil {
			return err
		}
		srv.Listener = l
		srv.started = true
		srv.lock.Unlock()
		err = srv.serveTCP(l)
		srv.lock.Lock() // to satisfy the defer at the top
		return err
	case "udp", "udp4", "udp6":
		a, err := net.ResolveUDPAddr(srv.Net, addr)
		if err != nil {
			return err
		}
		l, err := net.ListenUDP(srv.Net, a)
		if err != nil {
			return err
		}
		if e := setUDPSocketOptions(l); e != nil {
			return e
		}
		srv.PacketConn = l
		srv.started = true
		srv.lock.Unlock()
		err = srv.serveUDP(l)
		srv.lock.Lock() // to satisfy the defer at the top
		return err
	}
	return &Error{err: "bad network"}
}

func (srv *Server) ActivateAndServe() error {
	srv.lock.Lock()
	defer srv.lock.Unlock()
	if srv.started {
		return &Error{err: "server already started"}
	}
	pConn := srv.PacketConn
	l := srv.Listener
	if pConn != nil {
		if srv.UDPSize == 0 {
			srv.UDPSize = MinMsgSize
		}

		if t, ok := pConn.(*net.UDPConn); ok && t != nil {
			if e := setUDPSocketOptions(t); e != nil {
				return e
			}
			srv.started = true
			srv.lock.Unlock()
			e := srv.serveUDP(t)
			srv.lock.Lock() // to satisfy the defer at the top
			return e
		}
	}
	if l != nil {
		srv.started = true
		srv.lock.Unlock()
		e := srv.serveTCP(l)
		srv.lock.Lock() // to satisfy the defer at the top
		return e
	}
	return &Error{err: "bad listeners"}
}

func (srv *Server) Shutdown() error {
	srv.lock.Lock()
	if !srv.started {
		srv.lock.Unlock()
		return &Error{err: "server not started"}
	}
	srv.started = false
	srv.lock.Unlock()

	if srv.PacketConn != nil {
		srv.PacketConn.Close()
	}
	if srv.Listener != nil {
		srv.Listener.Close()
	}
	return nil
}

func (srv *Server) getReadTimeout() time.Duration {
	rtimeout := dnsTimeout
	if srv.ReadTimeout != 0 {
		rtimeout = srv.ReadTimeout
	}
	return rtimeout
}

func (srv *Server) serveTCP(l net.Listener) error {
	defer l.Close()

	if srv.NotifyStartedFunc != nil {
		srv.NotifyStartedFunc()
	}

	reader := Reader(&defaultReader{srv})
	if srv.DecorateReader != nil {
		reader = srv.DecorateReader(reader)
	}

	handler := srv.Handler
	if handler == nil {
		handler = DefaultServeMux
	}
	rtimeout := srv.getReadTimeout()

	for {
		rw, err := l.Accept()
		srv.lock.RLock()
		if !srv.started {
			srv.lock.RUnlock()
			return nil
		}
		srv.lock.RUnlock()
		if err != nil {
			if neterr, ok := err.(net.Error); ok && neterr.Temporary() {
				continue
			}
			return err
		}
		go func() {
			m, err := reader.ReadTCP(rw, rtimeout)
			if err != nil {
				rw.Close()
				return
			}
			srv.serve(rw.RemoteAddr(), handler, m, nil, nil, rw)
		}()
	}
}

func (srv *Server) serveUDP(l *net.UDPConn) error {
	defer l.Close()

	if srv.NotifyStartedFunc != nil {
		srv.NotifyStartedFunc()
	}

	reader := Reader(&defaultReader{srv})
	if srv.DecorateReader != nil {
		reader = srv.DecorateReader(reader)
	}

	handler := srv.Handler
	if handler == nil {
		handler = DefaultServeMux
	}
	rtimeout := srv.getReadTimeout()

	for {
		m, s, err := reader.ReadUDP(l, rtimeout)
		srv.lock.RLock()
		if !srv.started {
			srv.lock.RUnlock()
			return nil
		}
		srv.lock.RUnlock()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				continue
			}
			return err
		}
		if len(m) < headerSize {
			continue
		}
		go srv.serve(s.RemoteAddr(), handler, m, l, s, nil)
	}
}

func (srv *Server) serve(a net.Addr, h Handler, m []byte, u *net.UDPConn, s *SessionUDP, t net.Conn) {
	w := &response{tsigSecret: srv.TsigSecret, udp: u, tcp: t, remoteAddr: a, udpSession: s}
	if srv.DecorateWriter != nil {
		w.writer = srv.DecorateWriter(w)
	} else {
		w.writer = w
	}

	q := 0 // counter for the amount of TCP queries we get

	reader := Reader(&defaultReader{srv})
	if srv.DecorateReader != nil {
		reader = srv.DecorateReader(reader)
	}
Redo:
	req := new(Msg)
	err := req.Unpack(m)
	if err != nil { // Send a FormatError back
		x := new(Msg)
		x.SetRcodeFormatError(req)
		w.WriteMsg(x)
		goto Exit
	}
	if !srv.Unsafe && req.Response {
		goto Exit
	}

	w.tsigStatus = nil
	if w.tsigSecret != nil {
		if t := req.IsTsig(); t != nil {
			secret := t.Hdr.Name
			if _, ok := w.tsigSecret[secret]; !ok {
				w.tsigStatus = ErrKeyAlg
			}
			w.tsigStatus = TsigVerify(m, w.tsigSecret[secret], "", false)
			w.tsigTimersOnly = false
			w.tsigRequestMAC = req.Extra[len(req.Extra)-1].(*TSIG).MAC
		}
	}
	h.ServeDNS(w, req) // Writes back to the client

Exit:
	if w.tcp == nil {
		return
	}

	if q > maxTCPQueries { // close socket after this many queries
		w.Close()
		return
	}

	if w.hijacked {
		return // client calls Close()
	}
	if u != nil { // UDP, "close" and return
		w.Close()
		return
	}
	idleTimeout := tcpIdleTimeout
	if srv.IdleTimeout != nil {
		idleTimeout = srv.IdleTimeout()
	}
	m, err = reader.ReadTCP(w.tcp, idleTimeout)
	if err == nil {
		q++
		goto Redo
	}
	w.Close()
	return
}

func (srv *Server) readTCP(conn net.Conn, timeout time.Duration) ([]byte, error) {
	conn.SetReadDeadline(time.Now().Add(timeout))
	l := make([]byte, 2)
	n, err := conn.Read(l)
	if err != nil || n != 2 {
		if err != nil {
			return nil, err
		}
		return nil, ErrShortRead
	}
	length := binary.BigEndian.Uint16(l)
	if length == 0 {
		return nil, ErrShortRead
	}
	m := make([]byte, int(length))
	n, err = conn.Read(m[:int(length)])
	if err != nil || n == 0 {
		if err != nil {
			return nil, err
		}
		return nil, ErrShortRead
	}
	i := n
	for i < int(length) {
		j, err := conn.Read(m[i:int(length)])
		if err != nil {
			return nil, err
		}
		i += j
	}
	n = i
	m = m[:n]
	return m, nil
}

func (srv *Server) readUDP(conn *net.UDPConn, timeout time.Duration) ([]byte, *SessionUDP, error) {
	conn.SetReadDeadline(time.Now().Add(timeout))
	m := make([]byte, srv.UDPSize)
	n, s, err := ReadFromSessionUDP(conn, m)
	if err != nil {
		return nil, nil, err
	}
	m = m[:n]
	return m, s, nil
}

func (w *response) WriteMsg(m *Msg) (err error) {
	var data []byte
	if w.tsigSecret != nil { // if no secrets, dont check for the tsig (which is a longer check)
		if t := m.IsTsig(); t != nil {
			data, w.tsigRequestMAC, err = TsigGenerate(m, w.tsigSecret[t.Hdr.Name], w.tsigRequestMAC, w.tsigTimersOnly)
			if err != nil {
				return err
			}
			_, err = w.writer.Write(data)
			return err
		}
	}
	data, err = m.Pack()
	if err != nil {
		return err
	}
	_, err = w.writer.Write(data)
	return err
}

func (w *response) Write(m []byte) (int, error) {
	switch {
	case w.udp != nil:
		n, err := WriteToSessionUDP(w.udp, m, w.udpSession)
		return n, err
	case w.tcp != nil:
		lm := len(m)
		if lm < 2 {
			return 0, io.ErrShortBuffer
		}
		if lm > MaxMsgSize {
			return 0, &Error{err: "message too large"}
		}
		l := make([]byte, 2, 2+lm)
		binary.BigEndian.PutUint16(l, uint16(lm))
		m = append(l, m...)

		n, err := io.Copy(w.tcp, bytes.NewReader(m))
		return int(n), err
	}
	panic("not reached")
}

func (w *response) LocalAddr() net.Addr {
	if w.tcp != nil {
		return w.tcp.LocalAddr()
	}
	return w.udp.LocalAddr()
}

func (w *response) RemoteAddr() net.Addr { return w.remoteAddr }

func (w *response) TsigStatus() error { return w.tsigStatus }

func (w *response) TsigTimersOnly(b bool) { w.tsigTimersOnly = b }

func (w *response) Hijack() { w.hijacked = true }

func (w *response) Close() error {

	if w.tcp != nil {
		e := w.tcp.Close()
		w.tcp = nil
		return e
	}
	return nil
}
