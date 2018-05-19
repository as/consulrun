// Copyright 2011 The Go Authors. All rights reserved.

package proxy // import "golang.org/x/net/proxy"

import (
	"errors"
	"net"
	"net/url"
	"os"
	"sync"
)

type Dialer interface {
	Dial(network, addr string) (c net.Conn, err error)
}

type Auth struct {
	User, Password string
}

func FromEnvironment() Dialer {
	allProxy := allProxyEnv.Get()
	if len(allProxy) == 0 {
		return Direct
	}

	proxyURL, err := url.Parse(allProxy)
	if err != nil {
		return Direct
	}
	proxy, err := FromURL(proxyURL, Direct)
	if err != nil {
		return Direct
	}

	noProxy := noProxyEnv.Get()
	if len(noProxy) == 0 {
		return proxy
	}

	perHost := NewPerHost(proxy, Direct)
	perHost.AddFromString(noProxy)
	return perHost
}

var proxySchemes map[string]func(*url.URL, Dialer) (Dialer, error)

func RegisterDialerType(scheme string, f func(*url.URL, Dialer) (Dialer, error)) {
	if proxySchemes == nil {
		proxySchemes = make(map[string]func(*url.URL, Dialer) (Dialer, error))
	}
	proxySchemes[scheme] = f
}

func FromURL(u *url.URL, forward Dialer) (Dialer, error) {
	var auth *Auth
	if u.User != nil {
		auth = new(Auth)
		auth.User = u.User.Username()
		if p, ok := u.User.Password(); ok {
			auth.Password = p
		}
	}

	switch u.Scheme {
	case "socks5":
		return SOCKS5("tcp", u.Host, auth, forward)
	}

	if proxySchemes != nil {
		if f, ok := proxySchemes[u.Scheme]; ok {
			return f(u, forward)
		}
	}

	return nil, errors.New("proxy: unknown scheme: " + u.Scheme)
}

var (
	allProxyEnv = &envOnce{
		names: []string{"ALL_PROXY", "all_proxy"},
	}
	noProxyEnv = &envOnce{
		names: []string{"NO_PROXY", "no_proxy"},
	}
)

type envOnce struct {
	names []string
	once  sync.Once
	val   string
}

func (e *envOnce) Get() string {
	e.once.Do(e.init)
	return e.val
}

func (e *envOnce) init() {
	for _, n := range e.names {
		e.val = os.Getenv(n)
		if e.val != "" {
			return
		}
	}
}

func (e *envOnce) reset() {
	e.once = sync.Once{}
	e.val = ""
}
