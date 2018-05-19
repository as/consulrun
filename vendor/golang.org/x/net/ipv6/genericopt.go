// Copyright 2013 The Go Authors. All rights reserved.

package ipv6

import "syscall"

func (c *genericOpt) TrafficClass() (int, error) {
	if !c.ok() {
		return 0, syscall.EINVAL
	}
	so, ok := sockOpts[ssoTrafficClass]
	if !ok {
		return 0, errOpNoSupport
	}
	return so.GetInt(c.Conn)
}

func (c *genericOpt) SetTrafficClass(tclass int) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoTrafficClass]
	if !ok {
		return errOpNoSupport
	}
	return so.SetInt(c.Conn, tclass)
}

func (c *genericOpt) HopLimit() (int, error) {
	if !c.ok() {
		return 0, syscall.EINVAL
	}
	so, ok := sockOpts[ssoHopLimit]
	if !ok {
		return 0, errOpNoSupport
	}
	return so.GetInt(c.Conn)
}

func (c *genericOpt) SetHopLimit(hoplim int) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoHopLimit]
	if !ok {
		return errOpNoSupport
	}
	return so.SetInt(c.Conn, hoplim)
}
