// Copyright 2016 The Go Authors. All rights reserved.

// +build amd64,linux
// +build !gccgo

package unix

import "syscall"

//go:noescape
func gettimeofday(tv *Timeval) (err syscall.Errno)
