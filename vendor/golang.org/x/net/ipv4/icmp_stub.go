// Copyright 2014 The Go Authors. All rights reserved.

// +build !linux

package ipv4

const sizeofICMPFilter = 0x0

type icmpFilter struct {
}

func (f *icmpFilter) accept(typ ICMPType) {
}

func (f *icmpFilter) block(typ ICMPType) {
}

func (f *icmpFilter) setAll(block bool) {
}

func (f *icmpFilter) willBlock(typ ICMPType) bool {
	return false
}
