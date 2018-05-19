// Copyright 2017 The Go Authors. All rights reserved.

// +build darwin dragonfly freebsd linux netbsd openbsd solaris

package socket

func (h *cmsghdr) len() int { return int(h.Len) }
func (h *cmsghdr) lvl() int { return int(h.Level) }
func (h *cmsghdr) typ() int { return int(h.Type) }
