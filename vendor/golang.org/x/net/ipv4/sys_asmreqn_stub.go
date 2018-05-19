// Copyright 2014 The Go Authors. All rights reserved.

// +build !darwin,!freebsd,!linux

package ipv4

import (
	"net"

	"golang.org/x/net/internal/socket"
)

func (so *sockOpt) getIPMreqn(c *socket.Conn) (*net.Interface, error) {
	return nil, errOpNoSupport
}

func (so *sockOpt) setIPMreqn(c *socket.Conn, ifi *net.Interface, grp net.IP) error {
	return errOpNoSupport
}
