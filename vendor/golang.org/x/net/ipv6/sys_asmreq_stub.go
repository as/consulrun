// Copyright 2013 The Go Authors. All rights reserved.

// +build !darwin,!dragonfly,!freebsd,!linux,!netbsd,!openbsd,!solaris,!windows

package ipv6

import (
	"net"

	"golang.org/x/net/internal/socket"
)

func (so *sockOpt) setIPMreq(c *socket.Conn, ifi *net.Interface, grp net.IP) error {
	return errOpNoSupport
}
