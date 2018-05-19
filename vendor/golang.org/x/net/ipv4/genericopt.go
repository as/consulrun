// Copyright 2012 The Go Authors. All rights reserved.

package ipv4

import "syscall"

func (c *genericOpt) TOS() (int, error) {
	if !c.ok() {
		return 0, syscall.EINVAL
	}
	so, ok := sockOpts[ssoTOS]
	if !ok {
		return 0, errOpNoSupport
	}
	return so.GetInt(c.Conn)
}

func (c *genericOpt) SetTOS(tos int) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoTOS]
	if !ok {
		return errOpNoSupport
	}
	return so.SetInt(c.Conn, tos)
}

func (c *genericOpt) TTL() (int, error) {
	if !c.ok() {
		return 0, syscall.EINVAL
	}
	so, ok := sockOpts[ssoTTL]
	if !ok {
		return 0, errOpNoSupport
	}
	return so.GetInt(c.Conn)
}

func (c *genericOpt) SetTTL(ttl int) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoTTL]
	if !ok {
		return errOpNoSupport
	}
	return so.SetInt(c.Conn, ttl)
}
