/*
 *
 * Copyright 2017 gRPC authors.
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

package balancer

import (
	"errors"
	"net"
	"strings"

	"golang.org/x/net/context"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
)

var (
	m = make(map[string]Builder)
)

// Register registers the balancer builder to the balancer map.
// this builder.
func Register(b Builder) {
	m[strings.ToLower(b.Name())] = b
}

// Get returns the resolver builder registered with the given name.
// If no builder is register with the name, nil will be returned.
func Get(name string) Builder {
	if b, ok := m[strings.ToLower(name)]; ok {
		return b
	}
	return nil
}

//
//
//
type SubConn interface {

	//

	UpdateAddresses([]resolver.Address)

	Connect()
}

type NewSubConnOptions struct{}

//
type ClientConn interface {
	NewSubConn([]resolver.Address, NewSubConnOptions) (SubConn, error)

	RemoveSubConn(SubConn)

	//

	UpdateBalancerState(s connectivity.State, p Picker)

	ResolveNow(resolver.ResolveNowOption)

	Target() string
}

type BuildOptions struct {
	DialCreds credentials.TransportCredentials

	Dialer func(context.Context, string) (net.Conn, error)
}

type Builder interface {
	Build(cc ClientConn, opts BuildOptions) Balancer

	Name() string
}

type PickOptions struct{}

type DoneInfo struct {
	Err error

	BytesSent bool

	BytesReceived bool
}

var (
	ErrNoSubConnAvailable = errors.New("no SubConn is available")

	ErrTransientFailure = errors.New("all SubConns are in TransientFailure")
)

//
type Picker interface {

	//

	//

	//

	//

	Pick(ctx context.Context, opts PickOptions) (conn SubConn, done func(DoneInfo), err error)
}

//
//
type Balancer interface {
	HandleSubConnStateChange(sc SubConn, state connectivity.State)

	HandleResolvedAddrs([]resolver.Address, error)

	Close()
}
