/*
 *
 * Copyright 2016 gRPC authors.
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

// frames itself. It is the implementation of *grpc.Server.ServeHTTP.

package transport

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"golang.org/x/net/http2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func NewServerHandlerTransport(w http.ResponseWriter, r *http.Request) (ServerTransport, error) {
	if r.ProtoMajor != 2 {
		return nil, errors.New("gRPC requires HTTP/2")
	}
	if r.Method != "POST" {
		return nil, errors.New("invalid gRPC request method")
	}
	if !validContentType(r.Header.Get("Content-Type")) {
		return nil, errors.New("invalid gRPC request content-type")
	}
	if _, ok := w.(http.Flusher); !ok {
		return nil, errors.New("gRPC requires a ResponseWriter supporting http.Flusher")
	}
	if _, ok := w.(http.CloseNotifier); !ok {
		return nil, errors.New("gRPC requires a ResponseWriter supporting http.CloseNotifier")
	}

	st := &serverHandlerTransport{
		rw:       w,
		req:      r,
		closedCh: make(chan struct{}),
		writes:   make(chan func()),
	}

	if v := r.Header.Get("grpc-timeout"); v != "" {
		to, err := decodeTimeout(v)
		if err != nil {
			return nil, streamErrorf(codes.Internal, "malformed time-out: %v", err)
		}
		st.timeoutSet = true
		st.timeout = to
	}

	var metakv []string
	if r.Host != "" {
		metakv = append(metakv, ":authority", r.Host)
	}
	for k, vv := range r.Header {
		k = strings.ToLower(k)
		if isReservedHeader(k) && !isWhitelistedPseudoHeader(k) {
			continue
		}
		for _, v := range vv {
			v, err := decodeMetadataHeader(k, v)
			if err != nil {
				return nil, streamErrorf(codes.InvalidArgument, "malformed binary metadata: %v", err)
			}
			metakv = append(metakv, k, v)
		}
	}
	st.headerMD = metadata.Pairs(metakv...)

	return st, nil
}

type serverHandlerTransport struct {
	rw               http.ResponseWriter
	req              *http.Request
	timeoutSet       bool
	timeout          time.Duration
	didCommonHeaders bool

	headerMD metadata.MD

	closeOnce sync.Once
	closedCh  chan struct{} // closed on Close

	writes chan func()

	writeStatusMu sync.Mutex
}

func (ht *serverHandlerTransport) Close() error {
	ht.closeOnce.Do(ht.closeCloseChanOnce)
	return nil
}

func (ht *serverHandlerTransport) closeCloseChanOnce() { close(ht.closedCh) }

func (ht *serverHandlerTransport) RemoteAddr() net.Addr { return strAddr(ht.req.RemoteAddr) }

type strAddr string

func (a strAddr) Network() string {
	if a != "" {

		//

		return "tcp"
	}
	return ""
}

func (a strAddr) String() string { return string(a) }

func (ht *serverHandlerTransport) do(fn func()) error {

	select {
	case <-ht.closedCh:
		return ErrConnClosing
	default:
		select {
		case ht.writes <- fn:
			return nil
		case <-ht.closedCh:
			return ErrConnClosing
		}
	}
}

func (ht *serverHandlerTransport) WriteStatus(s *Stream, st *status.Status) error {
	ht.writeStatusMu.Lock()
	defer ht.writeStatusMu.Unlock()

	err := ht.do(func() {
		ht.writeCommonHeaders(s)

		ht.rw.(http.Flusher).Flush()

		h := ht.rw.Header()
		h.Set("Grpc-Status", fmt.Sprintf("%d", st.Code()))
		if m := st.Message(); m != "" {
			h.Set("Grpc-Message", encodeGrpcMessage(m))
		}

		if p := st.Proto(); p != nil && len(p.Details) > 0 {
			stBytes, err := proto.Marshal(p)
			if err != nil {

				panic(err)
			}

			h.Set("Grpc-Status-Details-Bin", encodeBinHeader(stBytes))
		}

		if md := s.Trailer(); len(md) > 0 {
			for k, vv := range md {

				if isReservedHeader(k) {
					continue
				}
				for _, v := range vv {

					h.Add(http2.TrailerPrefix+k, encodeMetadataHeader(k, v))
				}
			}
		}
	})

	if err == nil { // transport has not been closed
		ht.Close()
		close(ht.writes)
	}
	return err
}

func (ht *serverHandlerTransport) writeCommonHeaders(s *Stream) {
	if ht.didCommonHeaders {
		return
	}
	ht.didCommonHeaders = true

	h := ht.rw.Header()
	h["Date"] = nil // suppress Date to make tests happy; TODO: restore
	h.Set("Content-Type", "application/grpc")

	h.Add("Trailer", "Grpc-Status")
	h.Add("Trailer", "Grpc-Message")
	h.Add("Trailer", "Grpc-Status-Details-Bin")

	if s.sendCompress != "" {
		h.Set("Grpc-Encoding", s.sendCompress)
	}
}

func (ht *serverHandlerTransport) Write(s *Stream, hdr []byte, data []byte, opts *Options) error {
	return ht.do(func() {
		ht.writeCommonHeaders(s)
		ht.rw.Write(hdr)
		ht.rw.Write(data)
		if !opts.Delay {
			ht.rw.(http.Flusher).Flush()
		}
	})
}

func (ht *serverHandlerTransport) WriteHeader(s *Stream, md metadata.MD) error {
	return ht.do(func() {
		ht.writeCommonHeaders(s)
		h := ht.rw.Header()
		for k, vv := range md {

			if isReservedHeader(k) {
				continue
			}
			for _, v := range vv {
				v = encodeMetadataHeader(k, v)
				h.Add(k, v)
			}
		}
		ht.rw.WriteHeader(200)
		ht.rw.(http.Flusher).Flush()
	})
}

func (ht *serverHandlerTransport) HandleStreams(startStream func(*Stream), traceCtx func(context.Context, string) context.Context) {

	ctx := contextFromRequest(ht.req)
	var cancel context.CancelFunc
	if ht.timeoutSet {
		ctx, cancel = context.WithTimeout(ctx, ht.timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	requestOver := make(chan struct{})

	clientGone := ht.rw.(http.CloseNotifier).CloseNotify()
	go func() {
		select {
		case <-requestOver:
			return
		case <-ht.closedCh:
		case <-clientGone:
		}
		cancel()
	}()

	req := ht.req

	s := &Stream{
		id:           0, // irrelevant
		requestRead:  func(int) {},
		cancel:       cancel,
		buf:          newRecvBuffer(),
		st:           ht,
		method:       req.URL.Path,
		recvCompress: req.Header.Get("grpc-encoding"),
	}
	pr := &peer.Peer{
		Addr: ht.RemoteAddr(),
	}
	if req.TLS != nil {
		pr.AuthInfo = credentials.TLSInfo{State: *req.TLS}
	}
	ctx = metadata.NewIncomingContext(ctx, ht.headerMD)
	ctx = peer.NewContext(ctx, pr)
	s.ctx = newContextWithStream(ctx, s)
	s.trReader = &transportReader{
		reader:        &recvBufferReader{ctx: s.ctx, recv: s.buf},
		windowHandler: func(int) {},
	}

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)

		const readSize = 8196
		for buf := make([]byte, readSize); ; {
			n, err := req.Body.Read(buf)
			if n > 0 {
				s.buf.put(recvMsg{data: buf[:n:n]})
				buf = buf[n:]
			}
			if err != nil {
				s.buf.put(recvMsg{err: mapRecvMsgError(err)})
				return
			}
			if len(buf) == 0 {
				buf = make([]byte, readSize)
			}
		}
	}()

	startStream(s)

	ht.runStream()
	close(requestOver)

	req.Body.Close()
	<-readerDone
}

func (ht *serverHandlerTransport) runStream() {
	for {
		select {
		case fn, ok := <-ht.writes:
			if !ok {
				return
			}
			fn()
		case <-ht.closedCh:
			return
		}
	}
}

func (ht *serverHandlerTransport) Drain() {
	panic("Drain() is not implemented")
}

func mapRecvMsgError(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return err
	}
	if se, ok := err.(http2.StreamError); ok {
		if code, ok := http2ErrConvTab[se.Code]; ok {
			return StreamError{
				Code: code,
				Desc: se.Error(),
			}
		}
	}
	return connectionErrorf(true, err, err.Error())
}
