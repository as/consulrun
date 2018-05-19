// Copyright 2017 The Go Authors. All rights reserved.

// +build go1.9

package socket

import (
	"errors"
	"net"
	"os"
	"syscall"
)

type Conn struct {
	network string
	c       syscall.RawConn
}

func NewConn(c net.Conn) (*Conn, error) {
	var err error
	var cc Conn
	switch c := c.(type) {
	case *net.TCPConn:
		cc.network = "tcp"
		cc.c, err = c.SyscallConn()
	case *net.UDPConn:
		cc.network = "udp"
		cc.c, err = c.SyscallConn()
	case *net.IPConn:
		cc.network = "ip"
		cc.c, err = c.SyscallConn()
	default:
		return nil, errors.New("unknown connection type")
	}
	if err != nil {
		return nil, err
	}
	return &cc, nil
}

func (o *Option) get(c *Conn, b []byte) (int, error) {
	var operr error
	var n int
	fn := func(s uintptr) {
		n, operr = getsockopt(s, o.Level, o.Name, b)
	}
	if err := c.c.Control(fn); err != nil {
		return 0, err
	}
	return n, os.NewSyscallError("getsockopt", operr)
}

func (o *Option) set(c *Conn, b []byte) error {
	var operr error
	fn := func(s uintptr) {
		operr = setsockopt(s, o.Level, o.Name, b)
	}
	if err := c.c.Control(fn); err != nil {
		return err
	}
	return os.NewSyscallError("setsockopt", operr)
}
