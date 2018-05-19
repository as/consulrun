// Copyright 2015 The Go Authors. All rights reserved.

// +build darwin dragonfly freebsd linux netbsd openbsd solaris

package unix

const (
	R_OK = 0x4
	W_OK = 0x2
	X_OK = 0x1
)
