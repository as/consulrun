// Copyright 2014 The Go Authors. All rights reserved.

package ipv4

func (f *icmpFilter) accept(typ ICMPType) {
	f.Data &^= 1 << (uint32(typ) & 31)
}

func (f *icmpFilter) block(typ ICMPType) {
	f.Data |= 1 << (uint32(typ) & 31)
}

func (f *icmpFilter) setAll(block bool) {
	if block {
		f.Data = 1<<32 - 1
	} else {
		f.Data = 0
	}
}

func (f *icmpFilter) willBlock(typ ICMPType) bool {
	return f.Data&(1<<(uint32(typ)&31)) != 0
}
