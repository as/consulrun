// Copyright 2017 The Go Authors. All rights reserved.

// +build arm64 amd64 ppc64 ppc64le mips64 mips64le s390x
// +build darwin dragonfly freebsd linux netbsd openbsd

package socket

import "unsafe"

func (v *iovec) set(b []byte) {
	l := len(b)
	if l == 0 {
		return
	}
	v.Base = (*byte)(unsafe.Pointer(&b[0]))
	v.Len = uint64(l)
}