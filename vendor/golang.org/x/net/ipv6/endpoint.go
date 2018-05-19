// Copyright 2013 The Go Authors. All rights reserved.

package ipv6

import (
	"net"
	"syscall"
	"time"

	"golang.org/x/net/internal/socket"
)

type Conn struct {
	genericOpt
}

type genericOpt struct {
	*socket.Conn
}

func (c *genericOpt) ok() bool { return c != nil && c.Conn != nil }

func (c *Conn) PathMTU() (int, error) {
	if !c.ok() {
		return 0, syscall.EINVAL
	}
	so, ok := sockOpts[ssoPathMTU]
	if !ok {
		return 0, errOpNoSupport
	}
	_, mtu, err := so.getMTUInfo(c.Conn)
	if err != nil {
		return 0, err
	}
	return mtu, nil
}

func NewConn(c net.Conn) *Conn {
	cc, _ := socket.NewConn(c)
	return &Conn{
		genericOpt: genericOpt{Conn: cc},
	}
}

type PacketConn struct {
	genericOpt
	dgramOpt
	payloadHandler
}

type dgramOpt struct {
	*socket.Conn
}

func (c *dgramOpt) ok() bool { return c != nil && c.Conn != nil }

func (c *PacketConn) SetControlMessage(cf ControlFlags, on bool) error {
	if !c.payloadHandler.ok() {
		return syscall.EINVAL
	}
	return setControlMessage(c.dgramOpt.Conn, &c.payloadHandler.rawOpt, cf, on)
}

func (c *PacketConn) SetDeadline(t time.Time) error {
	if !c.payloadHandler.ok() {
		return syscall.EINVAL
	}
	return c.payloadHandler.SetDeadline(t)
}

func (c *PacketConn) SetReadDeadline(t time.Time) error {
	if !c.payloadHandler.ok() {
		return syscall.EINVAL
	}
	return c.payloadHandler.SetReadDeadline(t)
}

func (c *PacketConn) SetWriteDeadline(t time.Time) error {
	if !c.payloadHandler.ok() {
		return syscall.EINVAL
	}
	return c.payloadHandler.SetWriteDeadline(t)
}

func (c *PacketConn) Close() error {
	if !c.payloadHandler.ok() {
		return syscall.EINVAL
	}
	return c.payloadHandler.Close()
}

func NewPacketConn(c net.PacketConn) *PacketConn {
	cc, _ := socket.NewConn(c.(net.Conn))
	return &PacketConn{
		genericOpt:     genericOpt{Conn: cc},
		dgramOpt:       dgramOpt{Conn: cc},
		payloadHandler: payloadHandler{PacketConn: c, Conn: cc},
	}
}
