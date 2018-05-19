// Copyright 2014 The Go Authors.  All rights reserved.

// +build windows
// +build go1.4

package windows

import "syscall"

func Unsetenv(key string) error {

	return syscall.Unsetenv(key)
}
