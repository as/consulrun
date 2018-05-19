// Copyright 2013 The Go Authors. All rights reserved.

package ipv6

import (
	"net"

	"golang.org/x/net/internal/socket"
)

type payloadHandler struct {
	net.PacketConn
	*socket.Conn
	rawOpt
}

func (c *payloadHandler) ok() bool { return c != nil && c.PacketConn != nil && c.Conn != nil }
