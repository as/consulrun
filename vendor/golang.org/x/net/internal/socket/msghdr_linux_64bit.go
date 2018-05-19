// Copyright 2017 The Go Authors. All rights reserved.

// +build arm64 amd64 ppc64 ppc64le mips64 mips64le s390x
// +build linux

package socket

import "unsafe"

func (h *msghdr) setIov(vs []iovec) {
	l := len(vs)
	if l == 0 {
		return
	}
	h.Iov = &vs[0]
	h.Iovlen = uint64(l)
}

func (h *msghdr) setControl(b []byte) {
	h.Control = (*byte)(unsafe.Pointer(&b[0]))
	h.Controllen = uint64(len(b))
}
