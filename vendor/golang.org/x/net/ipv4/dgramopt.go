// Copyright 2012 The Go Authors. All rights reserved.

package ipv4

import (
	"net"
	"syscall"

	"golang.org/x/net/bpf"
)

func (c *dgramOpt) MulticastTTL() (int, error) {
	if !c.ok() {
		return 0, syscall.EINVAL
	}
	so, ok := sockOpts[ssoMulticastTTL]
	if !ok {
		return 0, errOpNoSupport
	}
	return so.GetInt(c.Conn)
}

func (c *dgramOpt) SetMulticastTTL(ttl int) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoMulticastTTL]
	if !ok {
		return errOpNoSupport
	}
	return so.SetInt(c.Conn, ttl)
}

func (c *dgramOpt) MulticastInterface() (*net.Interface, error) {
	if !c.ok() {
		return nil, syscall.EINVAL
	}
	so, ok := sockOpts[ssoMulticastInterface]
	if !ok {
		return nil, errOpNoSupport
	}
	return so.getMulticastInterface(c.Conn)
}

func (c *dgramOpt) SetMulticastInterface(ifi *net.Interface) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoMulticastInterface]
	if !ok {
		return errOpNoSupport
	}
	return so.setMulticastInterface(c.Conn, ifi)
}

func (c *dgramOpt) MulticastLoopback() (bool, error) {
	if !c.ok() {
		return false, syscall.EINVAL
	}
	so, ok := sockOpts[ssoMulticastLoopback]
	if !ok {
		return false, errOpNoSupport
	}
	on, err := so.GetInt(c.Conn)
	if err != nil {
		return false, err
	}
	return on == 1, nil
}

func (c *dgramOpt) SetMulticastLoopback(on bool) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoMulticastLoopback]
	if !ok {
		return errOpNoSupport
	}
	return so.SetInt(c.Conn, boolint(on))
}

func (c *dgramOpt) JoinGroup(ifi *net.Interface, group net.Addr) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoJoinGroup]
	if !ok {
		return errOpNoSupport
	}
	grp := netAddrToIP4(group)
	if grp == nil {
		return errMissingAddress
	}
	return so.setGroup(c.Conn, ifi, grp)
}

func (c *dgramOpt) LeaveGroup(ifi *net.Interface, group net.Addr) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoLeaveGroup]
	if !ok {
		return errOpNoSupport
	}
	grp := netAddrToIP4(group)
	if grp == nil {
		return errMissingAddress
	}
	return so.setGroup(c.Conn, ifi, grp)
}

func (c *dgramOpt) JoinSourceSpecificGroup(ifi *net.Interface, group, source net.Addr) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoJoinSourceGroup]
	if !ok {
		return errOpNoSupport
	}
	grp := netAddrToIP4(group)
	if grp == nil {
		return errMissingAddress
	}
	src := netAddrToIP4(source)
	if src == nil {
		return errMissingAddress
	}
	return so.setSourceGroup(c.Conn, ifi, grp, src)
}

func (c *dgramOpt) LeaveSourceSpecificGroup(ifi *net.Interface, group, source net.Addr) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoLeaveSourceGroup]
	if !ok {
		return errOpNoSupport
	}
	grp := netAddrToIP4(group)
	if grp == nil {
		return errMissingAddress
	}
	src := netAddrToIP4(source)
	if src == nil {
		return errMissingAddress
	}
	return so.setSourceGroup(c.Conn, ifi, grp, src)
}

func (c *dgramOpt) ExcludeSourceSpecificGroup(ifi *net.Interface, group, source net.Addr) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoBlockSourceGroup]
	if !ok {
		return errOpNoSupport
	}
	grp := netAddrToIP4(group)
	if grp == nil {
		return errMissingAddress
	}
	src := netAddrToIP4(source)
	if src == nil {
		return errMissingAddress
	}
	return so.setSourceGroup(c.Conn, ifi, grp, src)
}

func (c *dgramOpt) IncludeSourceSpecificGroup(ifi *net.Interface, group, source net.Addr) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoUnblockSourceGroup]
	if !ok {
		return errOpNoSupport
	}
	grp := netAddrToIP4(group)
	if grp == nil {
		return errMissingAddress
	}
	src := netAddrToIP4(source)
	if src == nil {
		return errMissingAddress
	}
	return so.setSourceGroup(c.Conn, ifi, grp, src)
}

func (c *dgramOpt) ICMPFilter() (*ICMPFilter, error) {
	if !c.ok() {
		return nil, syscall.EINVAL
	}
	so, ok := sockOpts[ssoICMPFilter]
	if !ok {
		return nil, errOpNoSupport
	}
	return so.getICMPFilter(c.Conn)
}

func (c *dgramOpt) SetICMPFilter(f *ICMPFilter) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoICMPFilter]
	if !ok {
		return errOpNoSupport
	}
	return so.setICMPFilter(c.Conn, f)
}

//
func (c *dgramOpt) SetBPF(filter []bpf.RawInstruction) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	so, ok := sockOpts[ssoAttachFilter]
	if !ok {
		return errOpNoSupport
	}
	return so.setBPF(c.Conn, filter)
}
