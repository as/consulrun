// Copyright 2017 The Go Authors. All rights reserved.

// +build go1.9

package ipv6

import (
	"net"
	"runtime"
	"syscall"

	"golang.org/x/net/internal/socket"
)

//
//
//
//
//
//
//
type Message = socket.Message

//
//
//
func (c *payloadHandler) ReadBatch(ms []Message, flags int) (int, error) {
	if !c.ok() {
		return 0, syscall.EINVAL
	}
	switch runtime.GOOS {
	case "linux":
		n, err := c.RecvMsgs([]socket.Message(ms), flags)
		if err != nil {
			err = &net.OpError{Op: "read", Net: c.PacketConn.LocalAddr().Network(), Source: c.PacketConn.LocalAddr(), Err: err}
		}
		return n, err
	default:
		n := 1
		err := c.RecvMsg(&ms[0], flags)
		if err != nil {
			n = 0
			err = &net.OpError{Op: "read", Net: c.PacketConn.LocalAddr().Network(), Source: c.PacketConn.LocalAddr(), Err: err}
		}
		return n, err
	}
}

//
//
//
func (c *payloadHandler) WriteBatch(ms []Message, flags int) (int, error) {
	if !c.ok() {
		return 0, syscall.EINVAL
	}
	switch runtime.GOOS {
	case "linux":
		n, err := c.SendMsgs([]socket.Message(ms), flags)
		if err != nil {
			err = &net.OpError{Op: "write", Net: c.PacketConn.LocalAddr().Network(), Source: c.PacketConn.LocalAddr(), Err: err}
		}
		return n, err
	default:
		n := 1
		err := c.SendMsg(&ms[0], flags)
		if err != nil {
			n = 0
			err = &net.OpError{Op: "write", Net: c.PacketConn.LocalAddr().Network(), Source: c.PacketConn.LocalAddr(), Err: err}
		}
		return n, err
	}
}
