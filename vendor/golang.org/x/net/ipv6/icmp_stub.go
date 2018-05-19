// Copyright 2013 The Go Authors. All rights reserved.

// +build !darwin,!dragonfly,!freebsd,!linux,!netbsd,!openbsd,!solaris,!windows

package ipv6

type icmpv6Filter struct {
}

func (f *icmpv6Filter) accept(typ ICMPType) {
}

func (f *icmpv6Filter) block(typ ICMPType) {
}

func (f *icmpv6Filter) setAll(block bool) {
}

func (f *icmpv6Filter) willBlock(typ ICMPType) bool {
	return false
}
