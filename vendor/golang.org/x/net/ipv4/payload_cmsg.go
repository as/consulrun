// Copyright 2012 The Go Authors. All rights reserved.

// +build !nacl,!plan9,!windows

package ipv4

import (
	"net"
	"syscall"
)

func (c *payloadHandler) ReadFrom(b []byte) (n int, cm *ControlMessage, src net.Addr, err error) {
	if !c.ok() {
		return 0, nil, nil, syscall.EINVAL
	}
	return c.readFrom(b)
}

func (c *payloadHandler) WriteTo(b []byte, cm *ControlMessage, dst net.Addr) (n int, err error) {
	if !c.ok() {
		return 0, syscall.EINVAL
	}
	return c.writeTo(b, cm, dst)
}
