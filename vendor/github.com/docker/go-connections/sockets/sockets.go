// Package sockets provides helper functions to create and configure Unix or TCP sockets.
package sockets

import (
	"errors"
	"net"
	"net/http"
	"time"
)

const defaultTimeout = 32 * time.Second

var ErrProtocolNotAvailable = errors.New("protocol not available")

func ConfigureTransport(tr *http.Transport, proto, addr string) error {
	switch proto {
	case "unix":
		return configureUnixTransport(tr, proto, addr)
	case "npipe":
		return configureNpipeTransport(tr, proto, addr)
	default:
		tr.Proxy = http.ProxyFromEnvironment
		dialer, err := DialerFromEnvironment(&net.Dialer{
			Timeout: defaultTimeout,
		})
		if err != nil {
			return err
		}
		tr.Dial = dialer.Dial
	}
	return nil
}
