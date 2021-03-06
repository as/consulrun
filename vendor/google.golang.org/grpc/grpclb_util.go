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

package grpc

import (
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/resolver"
)

//

//
type lbManualResolver struct {
	scheme string
	ccr    resolver.ClientConn

	ccb balancer.ClientConn
}

func (r *lbManualResolver) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOption) (resolver.Resolver, error) {
	r.ccr = cc
	return r, nil
}

func (r *lbManualResolver) Scheme() string {
	return r.scheme
}

func (r *lbManualResolver) ResolveNow(o resolver.ResolveNowOption) {
	r.ccb.ResolveNow(o)
}

func (*lbManualResolver) Close() {}

func (r *lbManualResolver) NewAddress(addrs []resolver.Address) {
	r.ccr.NewAddress(addrs)
}

func (r *lbManualResolver) NewServiceConfig(sc string) {
	r.ccr.NewServiceConfig(sc)
}
