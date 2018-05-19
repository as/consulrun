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

// Package base defines a balancer base that can be used to build balancers with
//
//
// to build round_robin like balancers with complex picking algorithms.
// builder from scratch.
//
package base

import (
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/resolver"
)

type PickerBuilder interface {
	Build(readySCs map[resolver.Address]balancer.SubConn) balancer.Picker
}

// NewBalancerBuilder returns a balancer builder. The balancers
// built by this builder will use the picker builder to build pickers.
func NewBalancerBuilder(name string, pb PickerBuilder) balancer.Builder {
	return &baseBuilder{
		name:          name,
		pickerBuilder: pb,
	}
}
