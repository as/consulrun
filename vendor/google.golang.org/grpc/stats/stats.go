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

package stats // import "google.golang.org/grpc/stats"

import (
	"net"
	"time"

	"golang.org/x/net/context"
)

type RPCStats interface {
	isRPCStats()

	IsClient() bool
}

type Begin struct {
	Client bool

	BeginTime time.Time

	FailFast bool
}

func (s *Begin) IsClient() bool { return s.Client }

func (s *Begin) isRPCStats() {}

type InPayload struct {
	Client bool

	Payload interface{}

	Data []byte

	Length int

	WireLength int

	RecvTime time.Time
}

func (s *InPayload) IsClient() bool { return s.Client }

func (s *InPayload) isRPCStats() {}

type InHeader struct {
	Client bool

	WireLength int

	FullMethod string

	RemoteAddr net.Addr

	LocalAddr net.Addr

	Compression string
}

func (s *InHeader) IsClient() bool { return s.Client }

func (s *InHeader) isRPCStats() {}

type InTrailer struct {
	Client bool

	WireLength int
}

func (s *InTrailer) IsClient() bool { return s.Client }

func (s *InTrailer) isRPCStats() {}

type OutPayload struct {
	Client bool

	Payload interface{}

	Data []byte

	Length int

	WireLength int

	SentTime time.Time
}

func (s *OutPayload) IsClient() bool { return s.Client }

func (s *OutPayload) isRPCStats() {}

type OutHeader struct {
	Client bool

	FullMethod string

	RemoteAddr net.Addr

	LocalAddr net.Addr

	Compression string
}

func (s *OutHeader) IsClient() bool { return s.Client }

func (s *OutHeader) isRPCStats() {}

type OutTrailer struct {
	Client bool

	WireLength int
}

func (s *OutTrailer) IsClient() bool { return s.Client }

func (s *OutTrailer) isRPCStats() {}

type End struct {
	Client bool

	EndTime time.Time

	Error error
}

func (s *End) IsClient() bool { return s.Client }

func (s *End) isRPCStats() {}

type ConnStats interface {
	isConnStats()

	IsClient() bool
}

type ConnBegin struct {
	Client bool
}

func (s *ConnBegin) IsClient() bool { return s.Client }

func (s *ConnBegin) isConnStats() {}

type ConnEnd struct {
	Client bool
}

func (s *ConnEnd) IsClient() bool { return s.Client }

func (s *ConnEnd) isConnStats() {}

type incomingTagsKey struct{}
type outgoingTagsKey struct{}

//
func SetTags(ctx context.Context, b []byte) context.Context {
	return context.WithValue(ctx, outgoingTagsKey{}, b)
}

//
func Tags(ctx context.Context) []byte {
	b, _ := ctx.Value(incomingTagsKey{}).([]byte)
	return b
}

//
func SetIncomingTags(ctx context.Context, b []byte) context.Context {
	return context.WithValue(ctx, incomingTagsKey{}, b)
}

//
func OutgoingTags(ctx context.Context) []byte {
	b, _ := ctx.Value(outgoingTagsKey{}).([]byte)
	return b
}

type incomingTraceKey struct{}
type outgoingTraceKey struct{}

//
func SetTrace(ctx context.Context, b []byte) context.Context {
	return context.WithValue(ctx, outgoingTraceKey{}, b)
}

//
func Trace(ctx context.Context) []byte {
	b, _ := ctx.Value(incomingTraceKey{}).([]byte)
	return b
}

func SetIncomingTrace(ctx context.Context, b []byte) context.Context {
	return context.WithValue(ctx, incomingTraceKey{}, b)
}

func OutgoingTrace(ctx context.Context) []byte {
	b, _ := ctx.Value(outgoingTraceKey{}).([]byte)
	return b
}
