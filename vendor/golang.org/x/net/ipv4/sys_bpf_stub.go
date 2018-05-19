// Copyright 2017 The Go Authors. All rights reserved.

// +build !linux

package ipv4

import (
	"golang.org/x/net/bpf"
	"golang.org/x/net/internal/socket"
)

func (so *sockOpt) setAttachFilter(c *socket.Conn, f []bpf.RawInstruction) error {
	return errOpNoSupport
}
