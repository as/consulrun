// Copyright 2017 The Go Authors. All rights reserved.

// +build freebsd netbsd openbsd

package socket

import "unsafe"

func probeProtocolStack() int {
	var p uintptr
	return int(unsafe.Sizeof(p))
}
