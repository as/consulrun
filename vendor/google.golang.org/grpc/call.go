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
	"io"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/net/trace"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/transport"
)

//
func recvResponse(ctx context.Context, dopts dialOptions, t transport.ClientTransport, c *callInfo, stream *transport.Stream, reply interface{}) (err error) {

	defer func() {
		if err != nil {
			if _, ok := err.(transport.ConnectionError); !ok {
				t.CloseStream(stream, err)
			}
		}
	}()
	p := &parser{r: stream}
	var inPayload *stats.InPayload
	if dopts.copts.StatsHandler != nil {
		inPayload = &stats.InPayload{
			Client: true,
		}
	}
	for {
		if c.maxReceiveMessageSize == nil {
			return status.Errorf(codes.Internal, "callInfo maxReceiveMessageSize field uninitialized(nil)")
		}

		var comp encoding.Compressor
		var dc Decompressor
		if rc := stream.RecvCompress(); dopts.dc != nil && dopts.dc.Type() == rc {
			dc = dopts.dc
		} else if rc != "" && rc != encoding.Identity {
			comp = encoding.GetCompressor(rc)
		}
		if err = recv(p, dopts.codec, stream, dc, reply, *c.maxReceiveMessageSize, inPayload, comp); err != nil {
			if err == io.EOF {
				break
			}
			return
		}
	}
	if inPayload != nil && err == io.EOF && stream.Status().Code() == codes.OK {

		dopts.copts.StatsHandler.HandleRPC(ctx, inPayload)
	}
	return nil
}

func sendRequest(ctx context.Context, dopts dialOptions, compressor Compressor, c *callInfo, callHdr *transport.CallHdr, stream *transport.Stream, t transport.ClientTransport, args interface{}, opts *transport.Options) (err error) {
	defer func() {
		if err != nil {

			if _, ok := err.(transport.ConnectionError); !ok {
				t.CloseStream(stream, err)
			}
		}
	}()
	var (
		outPayload *stats.OutPayload
	)
	if dopts.copts.StatsHandler != nil {
		outPayload = &stats.OutPayload{
			Client: true,
		}
	}

	var comp encoding.Compressor
	if ct := c.compressorType; ct != "" && ct != encoding.Identity {
		compressor = nil // Disable the legacy compressor.
		comp = encoding.GetCompressor(ct)
		if comp == nil {
			return status.Errorf(codes.Internal, "grpc: Compressor is not installed for grpc-encoding %q", ct)
		}
	}
	hdr, data, err := encode(dopts.codec, args, compressor, outPayload, comp)
	if err != nil {
		return err
	}
	if c.maxSendMessageSize == nil {
		return status.Errorf(codes.Internal, "callInfo maxSendMessageSize field uninitialized(nil)")
	}
	if len(data) > *c.maxSendMessageSize {
		return status.Errorf(codes.ResourceExhausted, "grpc: trying to send message larger than max (%d vs. %d)", len(data), *c.maxSendMessageSize)
	}
	err = t.Write(stream, hdr, data, opts)
	if err == nil && outPayload != nil {
		outPayload.SentTime = time.Now()
		dopts.copts.StatsHandler.HandleRPC(ctx, outPayload)
	}

	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (cc *ClientConn) Invoke(ctx context.Context, method string, args, reply interface{}, opts ...CallOption) error {
	if cc.dopts.unaryInt != nil {
		return cc.dopts.unaryInt(ctx, method, args, reply, cc, invoke, opts...)
	}
	return invoke(ctx, method, args, reply, cc, opts...)
}

//
func Invoke(ctx context.Context, method string, args, reply interface{}, cc *ClientConn, opts ...CallOption) error {
	return cc.Invoke(ctx, method, args, reply, opts...)
}

func invoke(ctx context.Context, method string, args, reply interface{}, cc *ClientConn, opts ...CallOption) (e error) {
	c := defaultCallInfo()
	mc := cc.GetMethodConfig(method)
	if mc.WaitForReady != nil {
		c.failFast = !*mc.WaitForReady
	}

	if mc.Timeout != nil && *mc.Timeout >= 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *mc.Timeout)
		defer cancel()
	}

	opts = append(cc.dopts.callOptions, opts...)
	for _, o := range opts {
		if err := o.before(c); err != nil {
			return toRPCErr(err)
		}
	}
	defer func() {
		for _, o := range opts {
			o.after(c)
		}
	}()

	c.maxSendMessageSize = getMaxSize(mc.MaxReqSize, c.maxSendMessageSize, defaultClientMaxSendMessageSize)
	c.maxReceiveMessageSize = getMaxSize(mc.MaxRespSize, c.maxReceiveMessageSize, defaultClientMaxReceiveMessageSize)

	if EnableTracing {
		c.traceInfo.tr = trace.New("grpc.Sent."+methodFamily(method), method)
		defer c.traceInfo.tr.Finish()
		c.traceInfo.firstLine.client = true
		if deadline, ok := ctx.Deadline(); ok {
			c.traceInfo.firstLine.deadline = deadline.Sub(time.Now())
		}
		c.traceInfo.tr.LazyLog(&c.traceInfo.firstLine, false)

		defer func() {
			if e != nil {
				c.traceInfo.tr.LazyLog(&fmtStringer{"%v", []interface{}{e}}, true)
				c.traceInfo.tr.SetError()
			}
		}()
	}
	ctx = newContextWithRPCInfo(ctx, c.failFast)
	sh := cc.dopts.copts.StatsHandler
	if sh != nil {
		ctx = sh.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: method, FailFast: c.failFast})
		begin := &stats.Begin{
			Client:    true,
			BeginTime: time.Now(),
			FailFast:  c.failFast,
		}
		sh.HandleRPC(ctx, begin)
		defer func() {
			end := &stats.End{
				Client:  true,
				EndTime: time.Now(),
				Error:   e,
			}
			sh.HandleRPC(ctx, end)
		}()
	}
	topts := &transport.Options{
		Last:  true,
		Delay: false,
	}
	callHdr := &transport.CallHdr{
		Host:   cc.authority,
		Method: method,
	}
	if c.creds != nil {
		callHdr.Creds = c.creds
	}
	if c.compressorType != "" {
		callHdr.SendCompress = c.compressorType
	} else if cc.dopts.cp != nil {
		callHdr.SendCompress = cc.dopts.cp.Type()
	}
	firstAttempt := true

	for {

		select {
		case <-ctx.Done():
			return toRPCErr(ctx.Err())
		default:
		}

		t, done, err := cc.getTransport(ctx, c.failFast)
		if err != nil {
			return err
		}
		stream, err := t.NewStream(ctx, callHdr)
		if err != nil {
			if done != nil {
				done(balancer.DoneInfo{Err: err})
			}

			if !c.failFast {
				continue
			}
			return toRPCErr(err)
		}
		c.stream = stream
		if c.traceInfo.tr != nil {
			c.traceInfo.tr.LazyLog(&payload{sent: true, msg: args}, true)
		}
		err = sendRequest(ctx, cc.dopts, cc.dopts.cp, c, callHdr, stream, t, args, topts)
		if err != nil {
			if done != nil {
				done(balancer.DoneInfo{
					Err:           err,
					BytesSent:     true,
					BytesReceived: stream.BytesReceived(),
				})
			}

			if !c.failFast && stream.Unprocessed() {

				if firstAttempt {

					firstAttempt = false
					continue
				}

			}
			return toRPCErr(err)
		}
		err = recvResponse(ctx, cc.dopts, t, c, stream, reply)
		if err != nil {
			if done != nil {
				done(balancer.DoneInfo{
					Err:           err,
					BytesSent:     true,
					BytesReceived: stream.BytesReceived(),
				})
			}
			if !c.failFast && stream.Unprocessed() {

				if firstAttempt {

					firstAttempt = false
					continue
				}

			}
			return toRPCErr(err)
		}
		if c.traceInfo.tr != nil {
			c.traceInfo.tr.LazyLog(&payload{sent: false, msg: reply}, true)
		}
		t.CloseStream(stream, nil)
		err = stream.Status().Err()
		if done != nil {
			done(balancer.DoneInfo{
				Err:           err,
				BytesSent:     true,
				BytesReceived: stream.BytesReceived(),
			})
		}
		if !c.failFast && stream.Unprocessed() {

			if firstAttempt {

				firstAttempt = false
				continue
			}
		}
		return err
	}
}
