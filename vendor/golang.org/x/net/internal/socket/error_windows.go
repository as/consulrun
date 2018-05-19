// Copyright 2017 The Go Authors. All rights reserved.

package socket

import "syscall"

var (
	errERROR_IO_PENDING error = syscall.ERROR_IO_PENDING
	errEINVAL           error = syscall.EINVAL
)

func errnoErr(errno syscall.Errno) error {
	switch errno {
	case 0:
		return nil
	case syscall.ERROR_IO_PENDING:
		return errERROR_IO_PENDING
	case syscall.EINVAL:
		return errEINVAL
	}
	return errno
}
