/*
 *
 * Copyright 2014 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package grpc

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"io/ioutil"

	"golang.org/x/net/context"
	"golang.org/x/net/http2"
	"golang.org/x/net/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
	"google.golang.org/grpc/transport"
)

const (
	defaultServerMaxReceiveMessageSize = 1024 * 1024 * 4
	defaultServerMaxSendMessageSize    = math.MaxInt32
)

type methodHandler func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor UnaryServerInterceptor) (interface{}, error)

type MethodDesc struct {
	MethodName string
	Handler    methodHandler
}

type ServiceDesc struct {
	ServiceName string

	HandlerType interface{}
	Methods     []MethodDesc
	Streams     []StreamDesc
	Metadata    interface{}
}

type service struct {
	server interface{} // the server for service methods
	md     map[string]*MethodDesc
	sd     map[string]*StreamDesc
	mdata  interface{}
}

type Server struct {
	opts options

	mu     sync.Mutex // guards following
	lis    map[net.Listener]bool
	conns  map[io.Closer]bool
	serve  bool
	drain  bool
	cv     *sync.Cond          // signaled when connections close for GracefulStop
	m      map[string]*service // service name -> service info
	events trace.EventLog

	quit     chan struct{}
	done     chan struct{}
	quitOnce sync.Once
	doneOnce sync.Once
	serveWG  sync.WaitGroup // counts active Serve goroutines for GracefulStop
}

type options struct {
	creds                 credentials.TransportCredentials
	codec                 Codec
	cp                    Compressor
	dc                    Decompressor
	unaryInt              UnaryServerInterceptor
	streamInt             StreamServerInterceptor
	inTapHandle           tap.ServerInHandle
	statsHandler          stats.Handler
	maxConcurrentStreams  uint32
	maxReceiveMessageSize int
	maxSendMessageSize    int
	useHandlerImpl        bool // use http.Handler-based server
	unknownStreamDesc     *StreamDesc
	keepaliveParams       keepalive.ServerParameters
	keepalivePolicy       keepalive.EnforcementPolicy
	initialWindowSize     int32
	initialConnWindowSize int32
	writeBufferSize       int
	readBufferSize        int
	connectionTimeout     time.Duration
}

var defaultServerOptions = options{
	maxReceiveMessageSize: defaultServerMaxReceiveMessageSize,
	maxSendMessageSize:    defaultServerMaxSendMessageSize,
	connectionTimeout:     120 * time.Second,
}

type ServerOption func(*options)

func WriteBufferSize(s int) ServerOption {
	return func(o *options) {
		o.writeBufferSize = s
	}
}

func ReadBufferSize(s int) ServerOption {
	return func(o *options) {
		o.readBufferSize = s
	}
}

func InitialWindowSize(s int32) ServerOption {
	return func(o *options) {
		o.initialWindowSize = s
	}
}

func InitialConnWindowSize(s int32) ServerOption {
	return func(o *options) {
		o.initialConnWindowSize = s
	}
}

func KeepaliveParams(kp keepalive.ServerParameters) ServerOption {
	return func(o *options) {
		o.keepaliveParams = kp
	}
}

func KeepaliveEnforcementPolicy(kep keepalive.EnforcementPolicy) ServerOption {
	return func(o *options) {
		o.keepalivePolicy = kep
	}
}

func CustomCodec(codec Codec) ServerOption {
	return func(o *options) {
		o.codec = codec
	}
}

//
func RPCCompressor(cp Compressor) ServerOption {
	return func(o *options) {
		o.cp = cp
	}
}

//
func RPCDecompressor(dc Decompressor) ServerOption {
	return func(o *options) {
		o.dc = dc
	}
}

func MaxMsgSize(m int) ServerOption {
	return MaxRecvMsgSize(m)
}

func MaxRecvMsgSize(m int) ServerOption {
	return func(o *options) {
		o.maxReceiveMessageSize = m
	}
}

func MaxSendMsgSize(m int) ServerOption {
	return func(o *options) {
		o.maxSendMessageSize = m
	}
}

func MaxConcurrentStreams(n uint32) ServerOption {
	return func(o *options) {
		o.maxConcurrentStreams = n
	}
}

func Creds(c credentials.TransportCredentials) ServerOption {
	return func(o *options) {
		o.creds = c
	}
}

func UnaryInterceptor(i UnaryServerInterceptor) ServerOption {
	return func(o *options) {
		if o.unaryInt != nil {
			panic("The unary server interceptor was already set and may not be reset.")
		}
		o.unaryInt = i
	}
}

func StreamInterceptor(i StreamServerInterceptor) ServerOption {
	return func(o *options) {
		if o.streamInt != nil {
			panic("The stream server interceptor was already set and may not be reset.")
		}
		o.streamInt = i
	}
}

func InTapHandle(h tap.ServerInHandle) ServerOption {
	return func(o *options) {
		if o.inTapHandle != nil {
			panic("The tap handle was already set and may not be reset.")
		}
		o.inTapHandle = h
	}
}

func StatsHandler(h stats.Handler) ServerOption {
	return func(o *options) {
		o.statsHandler = h
	}
}

func UnknownServiceHandler(streamHandler StreamHandler) ServerOption {
	return func(o *options) {
		o.unknownStreamDesc = &StreamDesc{
			StreamName: "unknown_service_handler",
			Handler:    streamHandler,

			ClientStreams: true,
			ServerStreams: true,
		}
	}
}

//
func ConnectionTimeout(d time.Duration) ServerOption {
	return func(o *options) {
		o.connectionTimeout = d
	}
}

func NewServer(opt ...ServerOption) *Server {
	opts := defaultServerOptions
	for _, o := range opt {
		o(&opts)
	}
	if opts.codec == nil {

		opts.codec = protoCodec{}
	}
	s := &Server{
		lis:   make(map[net.Listener]bool),
		opts:  opts,
		conns: make(map[io.Closer]bool),
		m:     make(map[string]*service),
		quit:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	s.cv = sync.NewCond(&s.mu)
	if EnableTracing {
		_, file, line, _ := runtime.Caller(1)
		s.events = trace.NewEventLog("grpc.Server", fmt.Sprintf("%s:%d", file, line))
	}
	return s
}

func (s *Server) printf(format string, a ...interface{}) {
	if s.events != nil {
		s.events.Printf(format, a...)
	}
}

func (s *Server) errorf(format string, a ...interface{}) {
	if s.events != nil {
		s.events.Errorf(format, a...)
	}
}

func (s *Server) RegisterService(sd *ServiceDesc, ss interface{}) {
	ht := reflect.TypeOf(sd.HandlerType).Elem()
	st := reflect.TypeOf(ss)
	if !st.Implements(ht) {
		grpclog.Fatalf("grpc: Server.RegisterService found the handler of type %v that does not satisfy %v", st, ht)
	}
	s.register(sd, ss)
}

func (s *Server) register(sd *ServiceDesc, ss interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.printf("RegisterService(%q)", sd.ServiceName)
	if s.serve {
		grpclog.Fatalf("grpc: Server.RegisterService after Server.Serve for %q", sd.ServiceName)
	}
	if _, ok := s.m[sd.ServiceName]; ok {
		grpclog.Fatalf("grpc: Server.RegisterService found duplicate service registration for %q", sd.ServiceName)
	}
	srv := &service{
		server: ss,
		md:     make(map[string]*MethodDesc),
		sd:     make(map[string]*StreamDesc),
		mdata:  sd.Metadata,
	}
	for i := range sd.Methods {
		d := &sd.Methods[i]
		srv.md[d.MethodName] = d
	}
	for i := range sd.Streams {
		d := &sd.Streams[i]
		srv.sd[d.StreamName] = d
	}
	s.m[sd.ServiceName] = srv
}

type MethodInfo struct {
	Name string

	IsClientStream bool

	IsServerStream bool
}

type ServiceInfo struct {
	Methods []MethodInfo

	Metadata interface{}
}

func (s *Server) GetServiceInfo() map[string]ServiceInfo {
	ret := make(map[string]ServiceInfo)
	for n, srv := range s.m {
		methods := make([]MethodInfo, 0, len(srv.md)+len(srv.sd))
		for m := range srv.md {
			methods = append(methods, MethodInfo{
				Name:           m,
				IsClientStream: false,
				IsServerStream: false,
			})
		}
		for m, d := range srv.sd {
			methods = append(methods, MethodInfo{
				Name:           m,
				IsClientStream: d.ClientStreams,
				IsServerStream: d.ServerStreams,
			})
		}

		ret[n] = ServiceInfo{
			Methods:  methods,
			Metadata: srv.mdata,
		}
	}
	return ret
}

var ErrServerStopped = errors.New("grpc: the server has been stopped")

func (s *Server) useTransportAuthenticator(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	if s.opts.creds == nil {
		return rawConn, nil, nil
	}
	return s.opts.creds.ServerHandshake(rawConn)
}

func (s *Server) Serve(lis net.Listener) error {
	s.mu.Lock()
	s.printf("serving")
	s.serve = true
	if s.lis == nil {

		s.mu.Unlock()
		lis.Close()
		return ErrServerStopped
	}

	s.serveWG.Add(1)
	defer func() {
		s.serveWG.Done()
		select {

		case <-s.quit:
			<-s.done
		default:
		}
	}()

	s.lis[lis] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.lis != nil && s.lis[lis] {
			lis.Close()
			delete(s.lis, lis)
		}
		s.mu.Unlock()
	}()

	var tempDelay time.Duration // how long to sleep on accept failure

	for {
		rawConn, err := lis.Accept()
		if err != nil {
			if ne, ok := err.(interface {
				Temporary() bool
			}); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				s.mu.Lock()
				s.printf("Accept error: %v; retrying in %v", err, tempDelay)
				s.mu.Unlock()
				timer := time.NewTimer(tempDelay)
				select {
				case <-timer.C:
				case <-s.quit:
					timer.Stop()
					return nil
				}
				continue
			}
			s.mu.Lock()
			s.printf("done serving; Accept = %v", err)
			s.mu.Unlock()

			select {
			case <-s.quit:
				return nil
			default:
			}
			return err
		}
		tempDelay = 0

		//

		s.serveWG.Add(1)
		go func() {
			s.handleRawConn(rawConn)
			s.serveWG.Done()
		}()
	}
}

func (s *Server) handleRawConn(rawConn net.Conn) {
	rawConn.SetDeadline(time.Now().Add(s.opts.connectionTimeout))
	conn, authInfo, err := s.useTransportAuthenticator(rawConn)
	if err != nil {
		s.mu.Lock()
		s.errorf("ServerHandshake(%q) failed: %v", rawConn.RemoteAddr(), err)
		s.mu.Unlock()
		grpclog.Warningf("grpc: Server.Serve failed to complete security handshake from %q: %v", rawConn.RemoteAddr(), err)

		if err != credentials.ErrConnDispatched {
			rawConn.Close()
		}
		rawConn.SetDeadline(time.Time{})
		return
	}

	s.mu.Lock()
	if s.conns == nil {
		s.mu.Unlock()
		conn.Close()
		return
	}
	s.mu.Unlock()

	var serve func()
	c := conn.(io.Closer)
	if s.opts.useHandlerImpl {
		serve = func() { s.serveUsingHandler(conn) }
	} else {

		st := s.newHTTP2Transport(conn, authInfo)
		if st == nil {
			return
		}
		c = st
		serve = func() { s.serveStreams(st) }
	}

	rawConn.SetDeadline(time.Time{})
	if !s.addConn(c) {
		return
	}
	go func() {
		serve()
		s.removeConn(c)
	}()
}

func (s *Server) newHTTP2Transport(c net.Conn, authInfo credentials.AuthInfo) transport.ServerTransport {
	config := &transport.ServerConfig{
		MaxStreams:            s.opts.maxConcurrentStreams,
		AuthInfo:              authInfo,
		InTapHandle:           s.opts.inTapHandle,
		StatsHandler:          s.opts.statsHandler,
		KeepaliveParams:       s.opts.keepaliveParams,
		KeepalivePolicy:       s.opts.keepalivePolicy,
		InitialWindowSize:     s.opts.initialWindowSize,
		InitialConnWindowSize: s.opts.initialConnWindowSize,
		WriteBufferSize:       s.opts.writeBufferSize,
		ReadBufferSize:        s.opts.readBufferSize,
	}
	st, err := transport.NewServerTransport("http2", c, config)
	if err != nil {
		s.mu.Lock()
		s.errorf("NewServerTransport(%q) failed: %v", c.RemoteAddr(), err)
		s.mu.Unlock()
		c.Close()
		grpclog.Warningln("grpc: Server.Serve failed to create ServerTransport: ", err)
		return nil
	}
	return st
}

func (s *Server) serveStreams(st transport.ServerTransport) {
	defer st.Close()
	var wg sync.WaitGroup
	st.HandleStreams(func(stream *transport.Stream) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handleStream(st, stream, s.traceInfo(st, stream))
		}()
	}, func(ctx context.Context, method string) context.Context {
		if !EnableTracing {
			return ctx
		}
		tr := trace.New("grpc.Recv."+methodFamily(method), method)
		return trace.NewContext(ctx, tr)
	})
	wg.Wait()
}

var _ http.Handler = (*Server)(nil)

//
//
func (s *Server) serveUsingHandler(conn net.Conn) {
	h2s := &http2.Server{
		MaxConcurrentStreams: s.opts.maxConcurrentStreams,
	}
	h2s.ServeConn(conn, &http2.ServeConnOpts{
		Handler: s,
	})
}

//
//
//
//
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	st, err := transport.NewServerHandlerTransport(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !s.addConn(st) {
		return
	}
	defer s.removeConn(st)
	s.serveStreams(st)
}

func (s *Server) traceInfo(st transport.ServerTransport, stream *transport.Stream) (trInfo *traceInfo) {
	tr, ok := trace.FromContext(stream.Context())
	if !ok {
		return nil
	}

	trInfo = &traceInfo{
		tr: tr,
	}
	trInfo.firstLine.client = false
	trInfo.firstLine.remoteAddr = st.RemoteAddr()

	if dl, ok := stream.Context().Deadline(); ok {
		trInfo.firstLine.deadline = dl.Sub(time.Now())
	}
	return trInfo
}

func (s *Server) addConn(c io.Closer) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conns == nil {
		c.Close()
		return false
	}
	if s.drain {

		c.(transport.ServerTransport).Drain()
	}
	s.conns[c] = true
	return true
}

func (s *Server) removeConn(c io.Closer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conns != nil {
		delete(s.conns, c)
		s.cv.Broadcast()
	}
}

func (s *Server) sendResponse(t transport.ServerTransport, stream *transport.Stream, msg interface{}, cp Compressor, opts *transport.Options, comp encoding.Compressor) error {
	var (
		outPayload *stats.OutPayload
	)
	if s.opts.statsHandler != nil {
		outPayload = &stats.OutPayload{}
	}
	hdr, data, err := encode(s.opts.codec, msg, cp, outPayload, comp)
	if err != nil {
		grpclog.Errorln("grpc: server failed to encode response: ", err)
		return err
	}
	if len(data) > s.opts.maxSendMessageSize {
		return status.Errorf(codes.ResourceExhausted, "grpc: trying to send message larger than max (%d vs. %d)", len(data), s.opts.maxSendMessageSize)
	}
	err = t.Write(stream, hdr, data, opts)
	if err == nil && outPayload != nil {
		outPayload.SentTime = time.Now()
		s.opts.statsHandler.HandleRPC(stream.Context(), outPayload)
	}
	return err
}

func (s *Server) processUnaryRPC(t transport.ServerTransport, stream *transport.Stream, srv *service, md *MethodDesc, trInfo *traceInfo) (err error) {
	sh := s.opts.statsHandler
	if sh != nil {
		begin := &stats.Begin{
			BeginTime: time.Now(),
		}
		sh.HandleRPC(stream.Context(), begin)
		defer func() {
			end := &stats.End{
				EndTime: time.Now(),
			}
			if err != nil && err != io.EOF {
				end.Error = toRPCErr(err)
			}
			sh.HandleRPC(stream.Context(), end)
		}()
	}
	if trInfo != nil {
		defer trInfo.tr.Finish()
		trInfo.firstLine.client = false
		trInfo.tr.LazyLog(&trInfo.firstLine, false)
		defer func() {
			if err != nil && err != io.EOF {
				trInfo.tr.LazyLog(&fmtStringer{"%v", []interface{}{err}}, true)
				trInfo.tr.SetError()
			}
		}()
	}

	var comp, decomp encoding.Compressor
	var cp Compressor
	var dc Decompressor

	if rc := stream.RecvCompress(); s.opts.dc != nil && s.opts.dc.Type() == rc {
		dc = s.opts.dc
	} else if rc != "" && rc != encoding.Identity {
		decomp = encoding.GetCompressor(rc)
		if decomp == nil {
			st := status.Newf(codes.Unimplemented, "grpc: Decompressor is not installed for grpc-encoding %q", rc)
			t.WriteStatus(stream, st)
			return st.Err()
		}
	}

	//

	if s.opts.cp != nil {
		cp = s.opts.cp
		stream.SetSendCompress(cp.Type())
	} else if rc := stream.RecvCompress(); rc != "" && rc != encoding.Identity {

		comp = encoding.GetCompressor(rc)
		if comp != nil {
			stream.SetSendCompress(rc)
		}
	}

	p := &parser{r: stream}
	pf, req, err := p.recvMsg(s.opts.maxReceiveMessageSize)
	if err == io.EOF {

		return err
	}
	if err == io.ErrUnexpectedEOF {
		err = status.Errorf(codes.Internal, io.ErrUnexpectedEOF.Error())
	}
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if e := t.WriteStatus(stream, st); e != nil {
				grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status %v", e)
			}
		} else {
			switch st := err.(type) {
			case transport.ConnectionError:

			case transport.StreamError:
				if e := t.WriteStatus(stream, status.New(st.Code, st.Desc)); e != nil {
					grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status %v", e)
				}
			default:
				panic(fmt.Sprintf("grpc: Unexpected error (%T) from recvMsg: %v", st, st))
			}
		}
		return err
	}
	if st := checkRecvPayload(pf, stream.RecvCompress(), dc != nil || decomp != nil); st != nil {
		if e := t.WriteStatus(stream, st); e != nil {
			grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status %v", e)
		}
		return st.Err()
	}
	var inPayload *stats.InPayload
	if sh != nil {
		inPayload = &stats.InPayload{
			RecvTime: time.Now(),
		}
	}
	df := func(v interface{}) error {
		if inPayload != nil {
			inPayload.WireLength = len(req)
		}
		if pf == compressionMade {
			var err error
			if dc != nil {
				req, err = dc.Do(bytes.NewReader(req))
				if err != nil {
					return status.Errorf(codes.Internal, err.Error())
				}
			} else {
				tmp, _ := decomp.Decompress(bytes.NewReader(req))
				req, err = ioutil.ReadAll(tmp)
				if err != nil {
					return status.Errorf(codes.Internal, "grpc: failed to decompress the received message %v", err)
				}
			}
		}
		if len(req) > s.opts.maxReceiveMessageSize {

			return status.Errorf(codes.ResourceExhausted, "grpc: received message larger than max (%d vs. %d)", len(req), s.opts.maxReceiveMessageSize)
		}
		if err := s.opts.codec.Unmarshal(req, v); err != nil {
			return status.Errorf(codes.Internal, "grpc: error unmarshalling request: %v", err)
		}
		if inPayload != nil {
			inPayload.Payload = v
			inPayload.Data = req
			inPayload.Length = len(req)
			sh.HandleRPC(stream.Context(), inPayload)
		}
		if trInfo != nil {
			trInfo.tr.LazyLog(&payload{sent: false, msg: v}, true)
		}
		return nil
	}
	reply, appErr := md.Handler(srv.server, stream.Context(), df, s.opts.unaryInt)
	if appErr != nil {
		appStatus, ok := status.FromError(appErr)
		if !ok {

			appErr = status.Error(convertCode(appErr), appErr.Error())
			appStatus, _ = status.FromError(appErr)
		}
		if trInfo != nil {
			trInfo.tr.LazyLog(stringer(appStatus.Message()), true)
			trInfo.tr.SetError()
		}
		if e := t.WriteStatus(stream, appStatus); e != nil {
			grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status: %v", e)
		}
		return appErr
	}
	if trInfo != nil {
		trInfo.tr.LazyLog(stringer("OK"), false)
	}
	opts := &transport.Options{
		Last:  true,
		Delay: false,
	}

	if err := s.sendResponse(t, stream, reply, cp, opts, comp); err != nil {
		if err == io.EOF {

			return err
		}
		if s, ok := status.FromError(err); ok {
			if e := t.WriteStatus(stream, s); e != nil {
				grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status: %v", e)
			}
		} else {
			switch st := err.(type) {
			case transport.ConnectionError:

			case transport.StreamError:
				if e := t.WriteStatus(stream, status.New(st.Code, st.Desc)); e != nil {
					grpclog.Warningf("grpc: Server.processUnaryRPC failed to write status %v", e)
				}
			default:
				panic(fmt.Sprintf("grpc: Unexpected error (%T) from sendResponse: %v", st, st))
			}
		}
		return err
	}
	if trInfo != nil {
		trInfo.tr.LazyLog(&payload{sent: true, msg: reply}, true)
	}

	return t.WriteStatus(stream, status.New(codes.OK, ""))
}

func (s *Server) processStreamingRPC(t transport.ServerTransport, stream *transport.Stream, srv *service, sd *StreamDesc, trInfo *traceInfo) (err error) {
	sh := s.opts.statsHandler
	if sh != nil {
		begin := &stats.Begin{
			BeginTime: time.Now(),
		}
		sh.HandleRPC(stream.Context(), begin)
		defer func() {
			end := &stats.End{
				EndTime: time.Now(),
			}
			if err != nil && err != io.EOF {
				end.Error = toRPCErr(err)
			}
			sh.HandleRPC(stream.Context(), end)
		}()
	}
	ss := &serverStream{
		t:     t,
		s:     stream,
		p:     &parser{r: stream},
		codec: s.opts.codec,
		maxReceiveMessageSize: s.opts.maxReceiveMessageSize,
		maxSendMessageSize:    s.opts.maxSendMessageSize,
		trInfo:                trInfo,
		statsHandler:          sh,
	}

	if rc := stream.RecvCompress(); s.opts.dc != nil && s.opts.dc.Type() == rc {
		ss.dc = s.opts.dc
	} else if rc != "" && rc != encoding.Identity {
		ss.decomp = encoding.GetCompressor(rc)
		if ss.decomp == nil {
			st := status.Newf(codes.Unimplemented, "grpc: Decompressor is not installed for grpc-encoding %q", rc)
			t.WriteStatus(ss.s, st)
			return st.Err()
		}
	}

	//

	if s.opts.cp != nil {
		ss.cp = s.opts.cp
		stream.SetSendCompress(s.opts.cp.Type())
	} else if rc := stream.RecvCompress(); rc != "" && rc != encoding.Identity {

		ss.comp = encoding.GetCompressor(rc)
		if ss.comp != nil {
			stream.SetSendCompress(rc)
		}
	}

	if trInfo != nil {
		trInfo.tr.LazyLog(&trInfo.firstLine, false)
		defer func() {
			ss.mu.Lock()
			if err != nil && err != io.EOF {
				ss.trInfo.tr.LazyLog(&fmtStringer{"%v", []interface{}{err}}, true)
				ss.trInfo.tr.SetError()
			}
			ss.trInfo.tr.Finish()
			ss.trInfo.tr = nil
			ss.mu.Unlock()
		}()
	}
	var appErr error
	var server interface{}
	if srv != nil {
		server = srv.server
	}
	if s.opts.streamInt == nil {
		appErr = sd.Handler(server, ss)
	} else {
		info := &StreamServerInfo{
			FullMethod:     stream.Method(),
			IsClientStream: sd.ClientStreams,
			IsServerStream: sd.ServerStreams,
		}
		appErr = s.opts.streamInt(server, ss, info, sd.Handler)
	}
	if appErr != nil {
		appStatus, ok := status.FromError(appErr)
		if !ok {
			switch err := appErr.(type) {
			case transport.StreamError:
				appStatus = status.New(err.Code, err.Desc)
			default:
				appStatus = status.New(convertCode(appErr), appErr.Error())
			}
			appErr = appStatus.Err()
		}
		if trInfo != nil {
			ss.mu.Lock()
			ss.trInfo.tr.LazyLog(stringer(appStatus.Message()), true)
			ss.trInfo.tr.SetError()
			ss.mu.Unlock()
		}
		t.WriteStatus(ss.s, appStatus)

		return appErr
	}
	if trInfo != nil {
		ss.mu.Lock()
		ss.trInfo.tr.LazyLog(stringer("OK"), false)
		ss.mu.Unlock()
	}
	return t.WriteStatus(ss.s, status.New(codes.OK, ""))

}

func (s *Server) handleStream(t transport.ServerTransport, stream *transport.Stream, trInfo *traceInfo) {
	sm := stream.Method()
	if sm != "" && sm[0] == '/' {
		sm = sm[1:]
	}
	pos := strings.LastIndex(sm, "/")
	if pos == -1 {
		if trInfo != nil {
			trInfo.tr.LazyLog(&fmtStringer{"Malformed method name %q", []interface{}{sm}}, true)
			trInfo.tr.SetError()
		}
		errDesc := fmt.Sprintf("malformed method name: %q", stream.Method())
		if err := t.WriteStatus(stream, status.New(codes.ResourceExhausted, errDesc)); err != nil {
			if trInfo != nil {
				trInfo.tr.LazyLog(&fmtStringer{"%v", []interface{}{err}}, true)
				trInfo.tr.SetError()
			}
			grpclog.Warningf("grpc: Server.handleStream failed to write status: %v", err)
		}
		if trInfo != nil {
			trInfo.tr.Finish()
		}
		return
	}
	service := sm[:pos]
	method := sm[pos+1:]
	srv, ok := s.m[service]
	if !ok {
		if unknownDesc := s.opts.unknownStreamDesc; unknownDesc != nil {
			s.processStreamingRPC(t, stream, nil, unknownDesc, trInfo)
			return
		}
		if trInfo != nil {
			trInfo.tr.LazyLog(&fmtStringer{"Unknown service %v", []interface{}{service}}, true)
			trInfo.tr.SetError()
		}
		errDesc := fmt.Sprintf("unknown service %v", service)
		if err := t.WriteStatus(stream, status.New(codes.Unimplemented, errDesc)); err != nil {
			if trInfo != nil {
				trInfo.tr.LazyLog(&fmtStringer{"%v", []interface{}{err}}, true)
				trInfo.tr.SetError()
			}
			grpclog.Warningf("grpc: Server.handleStream failed to write status: %v", err)
		}
		if trInfo != nil {
			trInfo.tr.Finish()
		}
		return
	}

	if md, ok := srv.md[method]; ok {
		s.processUnaryRPC(t, stream, srv, md, trInfo)
		return
	}
	if sd, ok := srv.sd[method]; ok {
		s.processStreamingRPC(t, stream, srv, sd, trInfo)
		return
	}
	if trInfo != nil {
		trInfo.tr.LazyLog(&fmtStringer{"Unknown method %v", []interface{}{method}}, true)
		trInfo.tr.SetError()
	}
	if unknownDesc := s.opts.unknownStreamDesc; unknownDesc != nil {
		s.processStreamingRPC(t, stream, nil, unknownDesc, trInfo)
		return
	}
	errDesc := fmt.Sprintf("unknown method %v", method)
	if err := t.WriteStatus(stream, status.New(codes.Unimplemented, errDesc)); err != nil {
		if trInfo != nil {
			trInfo.tr.LazyLog(&fmtStringer{"%v", []interface{}{err}}, true)
			trInfo.tr.SetError()
		}
		grpclog.Warningf("grpc: Server.handleStream failed to write status: %v", err)
	}
	if trInfo != nil {
		trInfo.tr.Finish()
	}
}

func (s *Server) Stop() {
	s.quitOnce.Do(func() {
		close(s.quit)
	})

	defer func() {
		s.serveWG.Wait()
		s.doneOnce.Do(func() {
			close(s.done)
		})
	}()

	s.mu.Lock()
	listeners := s.lis
	s.lis = nil
	st := s.conns
	s.conns = nil

	s.cv.Broadcast()
	s.mu.Unlock()

	for lis := range listeners {
		lis.Close()
	}
	for c := range st {
		c.Close()
	}

	s.mu.Lock()
	if s.events != nil {
		s.events.Finish()
		s.events = nil
	}
	s.mu.Unlock()
}

func (s *Server) GracefulStop() {
	s.quitOnce.Do(func() {
		close(s.quit)
	})

	defer func() {
		s.doneOnce.Do(func() {
			close(s.done)
		})
	}()

	s.mu.Lock()
	if s.conns == nil {
		s.mu.Unlock()
		return
	}
	for lis := range s.lis {
		lis.Close()
	}
	s.lis = nil
	if !s.drain {
		for c := range s.conns {
			c.(transport.ServerTransport).Drain()
		}
		s.drain = true
	}

	s.mu.Unlock()
	s.serveWG.Wait()
	s.mu.Lock()

	for len(s.conns) != 0 {
		s.cv.Wait()
	}
	s.conns = nil
	if s.events != nil {
		s.events.Finish()
		s.events = nil
	}
	s.mu.Unlock()
}

func init() {
	internal.TestingUseHandlerImpl = func(arg interface{}) {
		arg.(*Server).opts.useHandlerImpl = true
	}
}

func SetHeader(ctx context.Context, md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	stream, ok := transport.StreamFromContext(ctx)
	if !ok {
		return status.Errorf(codes.Internal, "grpc: failed to fetch the stream from the context %v", ctx)
	}
	return stream.SetHeader(md)
}

func SendHeader(ctx context.Context, md metadata.MD) error {
	stream, ok := transport.StreamFromContext(ctx)
	if !ok {
		return status.Errorf(codes.Internal, "grpc: failed to fetch the stream from the context %v", ctx)
	}
	t := stream.ServerTransport()
	if t == nil {
		grpclog.Fatalf("grpc: SendHeader: %v has no ServerTransport to send header metadata.", stream)
	}
	if err := t.WriteHeader(stream, md); err != nil {
		return toRPCErr(err)
	}
	return nil
}

func SetTrailer(ctx context.Context, md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	stream, ok := transport.StreamFromContext(ctx)
	if !ok {
		return status.Errorf(codes.Internal, "grpc: failed to fetch the stream from the context %v", ctx)
	}
	return stream.SetTrailer(md)
}
