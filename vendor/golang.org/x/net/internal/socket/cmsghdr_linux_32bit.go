// Copyright 2017 The Go Authors. All rights reserved.

// +build arm mips mipsle 386
// +build linux

package socket

func (h *cmsghdr) set(l, lvl, typ int) {
	h.Len = uint32(l)
	h.Level = int32(lvl)
	h.Type = int32(typ)
}
