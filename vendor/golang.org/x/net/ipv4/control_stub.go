// Copyright 2012 The Go Authors. All rights reserved.

// +build !darwin,!dragonfly,!freebsd,!linux,!netbsd,!openbsd,!solaris,!windows

package ipv4

import "golang.org/x/net/internal/socket"

func setControlMessage(c *socket.Conn, opt *rawOpt, cf ControlFlags, on bool) error {
	return errOpNoSupport
}
