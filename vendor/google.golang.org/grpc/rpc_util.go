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
	"compress/gzip"
	"encoding/binary"
	"io"
	"io/ioutil"
	"math"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/transport"
)

type Compressor interface {
	Do(w io.Writer, p []byte) error

	Type() string
}

type gzipCompressor struct {
	pool sync.Pool
}

func NewGZIPCompressor() Compressor {
	return &gzipCompressor{
		pool: sync.Pool{
			New: func() interface{} {
				return gzip.NewWriter(ioutil.Discard)
			},
		},
	}
}

func (c *gzipCompressor) Do(w io.Writer, p []byte) error {
	z := c.pool.Get().(*gzip.Writer)
	defer c.pool.Put(z)
	z.Reset(w)
	if _, err := z.Write(p); err != nil {
		return err
	}
	return z.Close()
}

func (c *gzipCompressor) Type() string {
	return "gzip"
}

type Decompressor interface {
	Do(r io.Reader) ([]byte, error)

	Type() string
}

type gzipDecompressor struct {
	pool sync.Pool
}

func NewGZIPDecompressor() Decompressor {
	return &gzipDecompressor{}
}

func (d *gzipDecompressor) Do(r io.Reader) ([]byte, error) {
	var z *gzip.Reader
	switch maybeZ := d.pool.Get().(type) {
	case nil:
		newZ, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		z = newZ
	case *gzip.Reader:
		z = maybeZ
		if err := z.Reset(r); err != nil {
			d.pool.Put(z)
			return nil, err
		}
	}

	defer func() {
		z.Close()
		d.pool.Put(z)
	}()
	return ioutil.ReadAll(z)
}

func (d *gzipDecompressor) Type() string {
	return "gzip"
}

type callInfo struct {
	compressorType        string
	failFast              bool
	stream                *transport.Stream
	traceInfo             traceInfo // in trace.go
	maxReceiveMessageSize *int
	maxSendMessageSize    *int
	creds                 credentials.PerRPCCredentials
}

func defaultCallInfo() *callInfo {
	return &callInfo{failFast: true}
}

type CallOption interface {
	before(*callInfo) error

	after(*callInfo)
}

type EmptyCallOption struct{}

func (EmptyCallOption) before(*callInfo) error { return nil }
func (EmptyCallOption) after(*callInfo)        {}

type beforeCall func(c *callInfo) error

func (o beforeCall) before(c *callInfo) error { return o(c) }
func (o beforeCall) after(c *callInfo)        {}

type afterCall func(c *callInfo)

func (o afterCall) before(c *callInfo) error { return nil }
func (o afterCall) after(c *callInfo)        { o(c) }

func Header(md *metadata.MD) CallOption {
	return afterCall(func(c *callInfo) {
		if c.stream != nil {
			*md, _ = c.stream.Header()
		}
	})
}

func Trailer(md *metadata.MD) CallOption {
	return afterCall(func(c *callInfo) {
		if c.stream != nil {
			*md = c.stream.Trailer()
		}
	})
}

func Peer(p *peer.Peer) CallOption {
	return afterCall(func(c *callInfo) {
		if c.stream != nil {
			if x, ok := peer.FromContext(c.stream.Context()); ok {
				*p = *x
			}
		}
	})
}

//
func FailFast(failFast bool) CallOption {
	return beforeCall(func(c *callInfo) error {
		c.failFast = failFast
		return nil
	})
}

func MaxCallRecvMsgSize(s int) CallOption {
	return beforeCall(func(o *callInfo) error {
		o.maxReceiveMessageSize = &s
		return nil
	})
}

func MaxCallSendMsgSize(s int) CallOption {
	return beforeCall(func(o *callInfo) error {
		o.maxSendMessageSize = &s
		return nil
	})
}

func PerRPCCredentials(creds credentials.PerRPCCredentials) CallOption {
	return beforeCall(func(c *callInfo) error {
		c.creds = creds
		return nil
	})
}

//
func UseCompressor(name string) CallOption {
	return beforeCall(func(c *callInfo) error {
		c.compressorType = name
		return nil
	})
}

type payloadFormat uint8

const (
	compressionNone payloadFormat = iota // no compression
	compressionMade
)

type parser struct {
	r io.Reader

	header [5]byte
}

//
//
func (p *parser) recvMsg(maxReceiveMessageSize int) (pf payloadFormat, msg []byte, err error) {
	if _, err := p.r.Read(p.header[:]); err != nil {
		return 0, nil, err
	}

	pf = payloadFormat(p.header[0])
	length := binary.BigEndian.Uint32(p.header[1:])

	if length == 0 {
		return pf, nil, nil
	}
	if int64(length) > int64(maxInt) {
		return 0, nil, status.Errorf(codes.ResourceExhausted, "grpc: received message larger than max length allowed on current machine (%d vs. %d)", length, maxInt)
	}
	if int(length) > maxReceiveMessageSize {
		return 0, nil, status.Errorf(codes.ResourceExhausted, "grpc: received message larger than max (%d vs. %d)", length, maxReceiveMessageSize)
	}

	msg = make([]byte, int(length))
	if _, err := p.r.Read(msg); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	return pf, msg, nil
}

func encode(c Codec, msg interface{}, cp Compressor, outPayload *stats.OutPayload, compressor encoding.Compressor) ([]byte, []byte, error) {
	var (
		b    []byte
		cbuf *bytes.Buffer
	)
	const (
		payloadLen = 1
		sizeLen    = 4
	)
	if msg != nil {
		var err error
		b, err = c.Marshal(msg)
		if err != nil {
			return nil, nil, status.Errorf(codes.Internal, "grpc: error while marshaling: %v", err.Error())
		}
		if outPayload != nil {
			outPayload.Payload = msg

			outPayload.Data = b
			outPayload.Length = len(b)
		}
		if compressor != nil || cp != nil {
			cbuf = new(bytes.Buffer)

			if compressor != nil {
				z, _ := compressor.Compress(cbuf)
				if _, err := z.Write(b); err != nil {
					return nil, nil, status.Errorf(codes.Internal, "grpc: error while compressing: %v", err.Error())
				}
				z.Close()
			} else {

				if err := cp.Do(cbuf, b); err != nil {
					return nil, nil, status.Errorf(codes.Internal, "grpc: error while compressing: %v", err.Error())
				}
			}
			b = cbuf.Bytes()
		}
	}
	if uint(len(b)) > math.MaxUint32 {
		return nil, nil, status.Errorf(codes.ResourceExhausted, "grpc: message too large (%d bytes)", len(b))
	}

	bufHeader := make([]byte, payloadLen+sizeLen)
	if compressor != nil || cp != nil {
		bufHeader[0] = byte(compressionMade)
	} else {
		bufHeader[0] = byte(compressionNone)
	}

	binary.BigEndian.PutUint32(bufHeader[payloadLen:], uint32(len(b)))
	if outPayload != nil {
		outPayload.WireLength = payloadLen + sizeLen + len(b)
	}
	return bufHeader, b, nil
}

func checkRecvPayload(pf payloadFormat, recvCompress string, haveCompressor bool) *status.Status {
	switch pf {
	case compressionNone:
	case compressionMade:
		if recvCompress == "" || recvCompress == encoding.Identity {
			return status.New(codes.Internal, "grpc: compressed flag set with identity or empty encoding")
		}
		if !haveCompressor {
			return status.Newf(codes.Unimplemented, "grpc: Decompressor is not installed for grpc-encoding %q", recvCompress)
		}
	default:
		return status.Newf(codes.Internal, "grpc: received unexpected payload format %d", pf)
	}
	return nil
}

func recv(p *parser, c Codec, s *transport.Stream, dc Decompressor, m interface{}, maxReceiveMessageSize int, inPayload *stats.InPayload, compressor encoding.Compressor) error {
	pf, d, err := p.recvMsg(maxReceiveMessageSize)
	if err != nil {
		return err
	}
	if inPayload != nil {
		inPayload.WireLength = len(d)
	}

	if st := checkRecvPayload(pf, s.RecvCompress(), compressor != nil || dc != nil); st != nil {
		return st.Err()
	}

	if pf == compressionMade {

		if dc != nil {
			d, err = dc.Do(bytes.NewReader(d))
			if err != nil {
				return status.Errorf(codes.Internal, "grpc: failed to decompress the received message %v", err)
			}
		} else {
			dcReader, err := compressor.Decompress(bytes.NewReader(d))
			if err != nil {
				return status.Errorf(codes.Internal, "grpc: failed to decompress the received message %v", err)
			}
			d, err = ioutil.ReadAll(dcReader)
			if err != nil {
				return status.Errorf(codes.Internal, "grpc: failed to decompress the received message %v", err)
			}
		}
	}
	if len(d) > maxReceiveMessageSize {

		return status.Errorf(codes.ResourceExhausted, "grpc: received message larger than max (%d vs. %d)", len(d), maxReceiveMessageSize)
	}
	if err := c.Unmarshal(d, m); err != nil {
		return status.Errorf(codes.Internal, "grpc: failed to unmarshal the received message %v", err)
	}
	if inPayload != nil {
		inPayload.RecvTime = time.Now()
		inPayload.Payload = m

		inPayload.Data = d
		inPayload.Length = len(d)
	}
	return nil
}

type rpcInfo struct {
	failfast bool
}

type rpcInfoContextKey struct{}

func newContextWithRPCInfo(ctx context.Context, failfast bool) context.Context {
	return context.WithValue(ctx, rpcInfoContextKey{}, &rpcInfo{failfast: failfast})
}

func rpcInfoFromContext(ctx context.Context) (s *rpcInfo, ok bool) {
	s, ok = ctx.Value(rpcInfoContextKey{}).(*rpcInfo)
	return
}

//
func Code(err error) codes.Code {
	if s, ok := status.FromError(err); ok {
		return s.Code()
	}
	return codes.Unknown
}

//
func ErrorDesc(err error) string {
	if s, ok := status.FromError(err); ok {
		return s.Message()
	}
	return err.Error()
}

//
func Errorf(c codes.Code, format string, a ...interface{}) error {
	return status.Errorf(c, format, a...)
}

//
//
const (
	SupportPackageIsVersion3 = true
	SupportPackageIsVersion4 = true
	SupportPackageIsVersion5 = true
)

const Version = "1.10.0-dev"

const grpcUA = "grpc-go/" + Version
