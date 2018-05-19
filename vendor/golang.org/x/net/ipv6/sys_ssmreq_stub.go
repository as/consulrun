// Copyright 2014 The Go Authors. All rights reserved.

// +build !darwin,!freebsd,!linux,!solaris

package ipv6

import (
	"net"

	"golang.org/x/net/internal/socket"
)

func (so *sockOpt) setGroupReq(c *socket.Conn, ifi *net.Interface, grp net.IP) error {
	return errOpNoSupport
}

func (so *sockOpt) setGroupSourceReq(c *socket.Conn, ifi *net.Interface, grp, src net.IP) error {
	return errOpNoSupport
}
