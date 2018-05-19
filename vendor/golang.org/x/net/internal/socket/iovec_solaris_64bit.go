// Copyright 2017 The Go Authors. All rights reserved.

// +build amd64
// +build solaris

package socket

import "unsafe"

func (v *iovec) set(b []byte) {
	l := len(b)
	if l == 0 {
		return
	}
	v.Base = (*int8)(unsafe.Pointer(&b[0]))
	v.Len = uint64(l)
}