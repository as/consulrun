// Copyright 2014 The Go Authors.  All rights reserved.

// +build go1.4

package unix

import "syscall"

func Unsetenv(key string) error {

	return syscall.Unsetenv(key)
}
