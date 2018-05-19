// Package sockets provides helper functions to create and configure Unix or TCP sockets.
package sockets

import (
	"crypto/tls"
	"net"
)

func NewTCPSocket(addr string, tlsConfig *tls.Config) (net.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		tlsConfig.NextProtos = []string{"http/1.1"}
		l = tls.NewListener(l, tlsConfig)
	}
	return l, nil
}
