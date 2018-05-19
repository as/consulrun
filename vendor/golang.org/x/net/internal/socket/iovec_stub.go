// Copyright 2017 The Go Authors. All rights reserved.

// +build !darwin,!dragonfly,!freebsd,!linux,!netbsd,!openbsd,!solaris

package socket

type iovec struct{}

func (v *iovec) set(b []byte) {}
