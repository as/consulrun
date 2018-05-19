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

package resolver

var (
	m = make(map[string]Builder)

	defaultScheme = "passthrough"
)

// Register registers the resolver builder to the resolver map.
// b.Scheme will be used as the scheme registered with this builder.
func Register(b Builder) {
	m[b.Scheme()] = b
}

// Get returns the resolver builder registered with the given scheme.
// If no builder is register with the scheme, the default scheme will
func Get(scheme string) Builder {
	if b, ok := m[scheme]; ok {
		return b
	}
	if b, ok := m[defaultScheme]; ok {
		return b
	}
	return nil
}

func SetDefaultScheme(scheme string) {
	defaultScheme = scheme
}

type AddressType uint8

const (
	Backend AddressType = iota

	GRPCLB
)

type Address struct {
	Addr string

	Type AddressType

	//

	ServerName string

	Metadata interface{}
}

// BuildOption includes additional information for the builder to create
type BuildOption struct {
	UserOptions interface{}
}

//
type ClientConn interface {
	NewAddress(addresses []Address)

	NewServiceConfig(serviceConfig string)
}

type Target struct {
	Scheme    string
	Authority string
	Endpoint  string
}

type Builder interface {

	//

	Build(target Target, cc ClientConn, opts BuildOption) (Resolver, error)

	Scheme() string
}

type ResolveNowOption struct{}

type Resolver interface {

	//

	ResolveNow(ResolveNowOption)

	Close()
}

// UnregisterForTesting removes the resolver builder with the given scheme from the
func UnregisterForTesting(scheme string) {
	delete(m, scheme)
}
