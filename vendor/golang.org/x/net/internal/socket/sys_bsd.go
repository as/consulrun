// Copyright 2017 The Go Authors. All rights reserved.

// +build darwin dragonfly freebsd openbsd

package socket

import "errors"

func recvmmsg(s uintptr, hs []mmsghdr, flags int) (int, error) {
	return 0, errors.New("not implemented")
}

func sendmmsg(s uintptr, hs []mmsghdr, flags int) (int, error) {
	return 0, errors.New("not implemented")
}
