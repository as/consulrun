// Copyright 2016 The Go Authors.  All rights reserved.

// +build gccgo,linux,sparc64

package unix

import "syscall"

func realSysconf(name int) int64

func sysconf(name int) (n int64, err syscall.Errno) {
	r := realSysconf(name)
	if r < 0 {
		return 0, syscall.GetErrno()
	}
	return r, 0
}
