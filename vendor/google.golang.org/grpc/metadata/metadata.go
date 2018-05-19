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

package metadata // import "google.golang.org/grpc/metadata"

import (
	"fmt"
	"strings"

	"golang.org/x/net/context"
)

func DecodeKeyValue(k, v string) (string, string, error) {
	return k, v, nil
}

type MD map[string][]string

//
//
func New(m map[string]string) MD {
	md := MD{}
	for k, val := range m {
		key := strings.ToLower(k)
		md[key] = append(md[key], val)
	}
	return md
}

//
//
func Pairs(kv ...string) MD {
	if len(kv)%2 == 1 {
		panic(fmt.Sprintf("metadata: Pairs got the odd number of input pairs for metadata: %d", len(kv)))
	}
	md := MD{}
	var key string
	for i, s := range kv {
		if i%2 == 0 {
			key = strings.ToLower(s)
			continue
		}
		md[key] = append(md[key], s)
	}
	return md
}

func (md MD) Len() int {
	return len(md)
}

func (md MD) Copy() MD {
	return Join(md)
}

func Join(mds ...MD) MD {
	out := MD{}
	for _, md := range mds {
		for k, v := range md {
			out[k] = append(out[k], v...)
		}
	}
	return out
}

type mdIncomingKey struct{}
type mdOutgoingKey struct{}

func NewIncomingContext(ctx context.Context, md MD) context.Context {
	return context.WithValue(ctx, mdIncomingKey{}, md)
}

func NewOutgoingContext(ctx context.Context, md MD) context.Context {
	return context.WithValue(ctx, mdOutgoingKey{}, md)
}

func FromIncomingContext(ctx context.Context) (md MD, ok bool) {
	md, ok = ctx.Value(mdIncomingKey{}).(MD)
	return
}

func FromOutgoingContext(ctx context.Context) (md MD, ok bool) {
	md, ok = ctx.Value(mdOutgoingKey{}).(MD)
	return
}
