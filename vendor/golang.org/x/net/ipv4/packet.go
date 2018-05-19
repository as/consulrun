// Copyright 2012 The Go Authors. All rights reserved.

package ipv4

import (
	"net"
	"syscall"

	"golang.org/x/net/internal/socket"
)

type packetHandler struct {
	*net.IPConn
	*socket.Conn
	rawOpt
}

func (c *packetHandler) ok() bool { return c != nil && c.IPConn != nil && c.Conn != nil }

func (c *packetHandler) ReadFrom(b []byte) (h *Header, p []byte, cm *ControlMessage, err error) {
	if !c.ok() {
		return nil, nil, nil, syscall.EINVAL
	}
	return c.readFrom(b)
}

func slicePacket(b []byte) (h, p []byte, err error) {
	if len(b) < HeaderLen {
		return nil, nil, errHeaderTooShort
	}
	hdrlen := int(b[0]&0x0f) << 2
	return b[:hdrlen], b[hdrlen:], nil
}

//
//
func (c *packetHandler) WriteTo(h *Header, p []byte, cm *ControlMessage) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	return c.writeTo(h, p, cm)
}
