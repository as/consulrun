package sockets

import (
	"net"
	"net/http"
	"time"

	"github.com/Microsoft/go-winio"
)

func configureUnixTransport(tr *http.Transport, proto, addr string) error {
	return ErrProtocolNotAvailable
}

func configureNpipeTransport(tr *http.Transport, proto, addr string) error {

	tr.DisableCompression = true
	tr.Dial = func(_, _ string) (net.Conn, error) {
		return DialPipe(addr, defaultTimeout)
	}
	return nil
}

func DialPipe(addr string, timeout time.Duration) (net.Conn, error) {
	return winio.DialPipe(addr, &timeout)
}
