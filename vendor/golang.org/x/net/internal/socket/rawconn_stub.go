// Copyright 2017 The Go Authors. All rights reserved.

// +build !go1.9

package socket

import "errors"

func (c *Conn) recvMsg(m *Message, flags int) error {
	return errors.New("not implemented")
}

func (c *Conn) sendMsg(m *Message, flags int) error {
	return errors.New("not implemented")
}

func (c *Conn) recvMsgs(ms []Message, flags int) (int, error) {
	return 0, errors.New("not implemented")
}

func (c *Conn) sendMsgs(ms []Message, flags int) (int, error) {
	return 0, errors.New("not implemented")
}
