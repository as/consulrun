// Copyright 2015 The Go Authors.  All rights reserved.

// +build gccgo,linux,amd64

package unix

import "syscall"

func realGettimeofday(*Timeval, *byte) int32

func gettimeofday(tv *Timeval) (err syscall.Errno) {
	r := realGettimeofday(tv, nil)
	if r < 0 {
		return syscall.GetErrno()
	}
	return 0
}
