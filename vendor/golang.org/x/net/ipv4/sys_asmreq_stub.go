// Copyright 2017 The Go Authors. All rights reserved.

// +build !darwin,!dragonfly,!freebsd,!netbsd,!openbsd,!solaris,!windows

package ipv4

import (
	"net"

	"golang.org/x/net/internal/socket"
)

func (so *sockOpt) setIPMreq(c *socket.Conn, ifi *net.Interface, grp net.IP) error {
	return errOpNoSupport
}

func (so *sockOpt) getMulticastIf(c *socket.Conn) (*net.Interface, error) {
	return nil, errOpNoSupport
}

func (so *sockOpt) setMulticastIf(c *socket.Conn, ifi *net.Interface) error {
	return errOpNoSupport
}
