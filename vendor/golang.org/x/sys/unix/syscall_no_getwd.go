// Copyright 2013 The Go Authors. All rights reserved.

// +build dragonfly freebsd netbsd openbsd

package unix

const ImplementsGetwd = false

func Getwd() (string, error) { return "", ENOTSUP }
