// Copyright 2014 The Go Authors. All rights reserved.

// +build !darwin,!dragonfly,!freebsd,!linux,!netbsd,!openbsd,!solaris,!windows

package ipv6

var (
	ctlOpts = [ctlMax]ctlOpt{}

	sockOpts = map[int]*sockOpt{}
)
