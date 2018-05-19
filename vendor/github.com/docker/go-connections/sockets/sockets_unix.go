// +build !windows

package sockets

import (
	"fmt"
	"net"
	"net/http"
	"syscall"
	"time"
)

const maxUnixSocketPathSize = len(syscall.RawSockaddrUnix{}.Path)

func configureUnixTransport(tr *http.Transport, proto, addr string) error {
	if len(addr) > maxUnixSocketPathSize {
		return fmt.Errorf("Unix socket path %q is too long", addr)
	}

	tr.DisableCompression = true
	tr.Dial = func(_, _ string) (net.Conn, error) {
		return net.DialTimeout(proto, addr, defaultTimeout)
	}
	return nil
}

func configureNpipeTransport(tr *http.Transport, proto, addr string) error {
	return ErrProtocolNotAvailable
}

func DialPipe(_ string, _ time.Duration) (net.Conn, error) {
	return nil, syscall.EAFNOSUPPORT
}
