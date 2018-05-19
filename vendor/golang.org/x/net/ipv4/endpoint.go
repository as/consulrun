// Copyright 2012 The Go Authors. All rights reserved.

package ipv4

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
	return c.payloadHandler.PacketConn.SetDeadline(t)
}

func (c *PacketConn) SetReadDeadline(t time.Time) error {
	if !c.payloadHandler.ok() {
		return syscall.EINVAL
	}
	return c.payloadHandler.PacketConn.SetReadDeadline(t)
}

func (c *PacketConn) SetWriteDeadline(t time.Time) error {
	if !c.payloadHandler.ok() {
		return syscall.EINVAL
	}
	return c.payloadHandler.PacketConn.SetWriteDeadline(t)
}

func (c *PacketConn) Close() error {
	if !c.payloadHandler.ok() {
		return syscall.EINVAL
	}
	return c.payloadHandler.PacketConn.Close()
}

func NewPacketConn(c net.PacketConn) *PacketConn {
	cc, _ := socket.NewConn(c.(net.Conn))
	p := &PacketConn{
		genericOpt:     genericOpt{Conn: cc},
		dgramOpt:       dgramOpt{Conn: cc},
		payloadHandler: payloadHandler{PacketConn: c, Conn: cc},
	}
	return p
}

type RawConn struct {
	genericOpt
	dgramOpt
	packetHandler
}

func (c *RawConn) SetControlMessage(cf ControlFlags, on bool) error {
	if !c.packetHandler.ok() {
		return syscall.EINVAL
	}
	return setControlMessage(c.dgramOpt.Conn, &c.packetHandler.rawOpt, cf, on)
}

func (c *RawConn) SetDeadline(t time.Time) error {
	if !c.packetHandler.ok() {
		return syscall.EINVAL
	}
	return c.packetHandler.IPConn.SetDeadline(t)
}

func (c *RawConn) SetReadDeadline(t time.Time) error {
	if !c.packetHandler.ok() {
		return syscall.EINVAL
	}
	return c.packetHandler.IPConn.SetReadDeadline(t)
}

func (c *RawConn) SetWriteDeadline(t time.Time) error {
	if !c.packetHandler.ok() {
		return syscall.EINVAL
	}
	return c.packetHandler.IPConn.SetWriteDeadline(t)
}

func (c *RawConn) Close() error {
	if !c.packetHandler.ok() {
		return syscall.EINVAL
	}
	return c.packetHandler.IPConn.Close()
}

func NewRawConn(c net.PacketConn) (*RawConn, error) {
	cc, err := socket.NewConn(c.(net.Conn))
	if err != nil {
		return nil, err
	}
	r := &RawConn{
		genericOpt:    genericOpt{Conn: cc},
		dgramOpt:      dgramOpt{Conn: cc},
		packetHandler: packetHandler{IPConn: c.(*net.IPConn), Conn: cc},
	}
	so, ok := sockOpts[ssoHeaderPrepend]
	if !ok {
		return nil, errOpNoSupport
	}
	if err := so.SetInt(r.dgramOpt.Conn, boolint(true)); err != nil {
		return nil, err
	}
	return r, nil
}
