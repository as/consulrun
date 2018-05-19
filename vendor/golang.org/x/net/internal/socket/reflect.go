// Copyright 2017 The Go Authors. All rights reserved.

// +build !go1.9

package socket

import (
	"errors"
	"net"
	"os"
	"reflect"
	"runtime"
)

type Conn struct {
	c net.Conn
}

func NewConn(c net.Conn) (*Conn, error) {
	return &Conn{c: c}, nil
}

func (o *Option) get(c *Conn, b []byte) (int, error) {
	s, err := socketOf(c.c)
	if err != nil {
		return 0, err
	}
	n, err := getsockopt(s, o.Level, o.Name, b)
	return n, os.NewSyscallError("getsockopt", err)
}

func (o *Option) set(c *Conn, b []byte) error {
	s, err := socketOf(c.c)
	if err != nil {
		return err
	}
	return os.NewSyscallError("setsockopt", setsockopt(s, o.Level, o.Name, b))
}

func socketOf(c net.Conn) (uintptr, error) {
	switch c.(type) {
	case *net.TCPConn, *net.UDPConn, *net.IPConn:
		v := reflect.ValueOf(c)
		switch e := v.Elem(); e.Kind() {
		case reflect.Struct:
			fd := e.FieldByName("conn").FieldByName("fd")
			switch e := fd.Elem(); e.Kind() {
			case reflect.Struct:
				sysfd := e.FieldByName("sysfd")
				if runtime.GOOS == "windows" {
					return uintptr(sysfd.Uint()), nil
				}
				return uintptr(sysfd.Int()), nil
			}
		}
	}
	return 0, errors.New("invalid type")
}
