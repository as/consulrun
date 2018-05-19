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

package peer

import (
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc/credentials"
)

type Peer struct {
	Addr net.Addr

	AuthInfo credentials.AuthInfo
}

type peerKey struct{}

func NewContext(ctx context.Context, p *Peer) context.Context {
	return context.WithValue(ctx, peerKey{}, p)
}

func FromContext(ctx context.Context) (p *Peer, ok bool) {
	p, ok = ctx.Value(peerKey{}).(*Peer)
	return
}
