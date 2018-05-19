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
	"golang.org/x/net/context"
)

type UnaryInvoker func(ctx context.Context, method string, req, reply interface{}, cc *ClientConn, opts ...CallOption) error

type UnaryClientInterceptor func(ctx context.Context, method string, req, reply interface{}, cc *ClientConn, invoker UnaryInvoker, opts ...CallOption) error

type Streamer func(ctx context.Context, desc *StreamDesc, cc *ClientConn, method string, opts ...CallOption) (ClientStream, error)

type StreamClientInterceptor func(ctx context.Context, desc *StreamDesc, cc *ClientConn, method string, streamer Streamer, opts ...CallOption) (ClientStream, error)

type UnaryServerInfo struct {
	Server interface{}

	FullMethod string
}

type UnaryHandler func(ctx context.Context, req interface{}) (interface{}, error)

type UnaryServerInterceptor func(ctx context.Context, req interface{}, info *UnaryServerInfo, handler UnaryHandler) (resp interface{}, err error)

type StreamServerInfo struct {
	FullMethod string

	IsClientStream bool

	IsServerStream bool
}

type StreamServerInterceptor func(srv interface{}, ss ServerStream, info *StreamServerInfo, handler StreamHandler) error
