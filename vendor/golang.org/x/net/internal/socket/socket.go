// Copyright 2017 The Go Authors. All rights reserved.

package socket // import "golang.org/x/net/internal/socket"

import (
	"errors"
	"net"
	"unsafe"
)

type Option struct {
	Level int // level
	Name  int // name; must be equal or greater than 1
	Len   int // length of value in bytes; must be equal or greater than 1
}

func (o *Option) Get(c *Conn, b []byte) (int, error) {
	if o.Name < 1 || o.Len < 1 {
		return 0, errors.New("invalid option")
	}
	if len(b) < o.Len {
		return 0, errors.New("short buffer")
	}
	return o.get(c, b)
}

//
func (o *Option) GetInt(c *Conn) (int, error) {
	if o.Len != 1 && o.Len != 4 {
		return 0, errors.New("invalid option")
	}
	var b []byte
	var bb [4]byte
	if o.Len == 1 {
		b = bb[:1]
	} else {
		b = bb[:4]
	}
	n, err := o.get(c, b)
	if err != nil {
		return 0, err
	}
	if n != o.Len {
		return 0, errors.New("invalid option length")
	}
	if o.Len == 1 {
		return int(b[0]), nil
	}
	return int(NativeEndian.Uint32(b[:4])), nil
}

func (o *Option) Set(c *Conn, b []byte) error {
	if o.Name < 1 || o.Len < 1 {
		return errors.New("invalid option")
	}
	if len(b) < o.Len {
		return errors.New("short buffer")
	}
	return o.set(c, b)
}

//
func (o *Option) SetInt(c *Conn, v int) error {
	if o.Len != 1 && o.Len != 4 {
		return errors.New("invalid option")
	}
	var b []byte
	if o.Len == 1 {
		b = []byte{byte(v)}
	} else {
		var bb [4]byte
		NativeEndian.PutUint32(bb[:o.Len], uint32(v))
		b = bb[:4]
	}
	return o.set(c, b)
}

func controlHeaderLen() int {
	return roundup(sizeofCmsghdr)
}

func controlMessageLen(dataLen int) int {
	return roundup(sizeofCmsghdr) + dataLen
}

func ControlMessageSpace(dataLen int) int {
	return roundup(sizeofCmsghdr) + roundup(dataLen)
}

//
//
type ControlMessage []byte

func (m ControlMessage) Data(dataLen int) []byte {
	l := controlHeaderLen()
	if len(m) < l || len(m) < l+dataLen {
		return nil
	}
	return m[l : l+dataLen]
}

//
func (m ControlMessage) Next(dataLen int) ControlMessage {
	l := ControlMessageSpace(dataLen)
	if len(m) < l {
		return nil
	}
	return m[l:]
}

func (m ControlMessage) MarshalHeader(lvl, typ, dataLen int) error {
	if len(m) < controlHeaderLen() {
		return errors.New("short message")
	}
	h := (*cmsghdr)(unsafe.Pointer(&m[0]))
	h.set(controlMessageLen(dataLen), lvl, typ)
	return nil
}

func (m ControlMessage) ParseHeader() (lvl, typ, dataLen int, err error) {
	l := controlHeaderLen()
	if len(m) < l {
		return 0, 0, 0, errors.New("short message")
	}
	h := (*cmsghdr)(unsafe.Pointer(&m[0]))
	return h.lvl(), h.typ(), int(uint64(h.len()) - uint64(l)), nil
}

func (m ControlMessage) Marshal(lvl, typ int, data []byte) (ControlMessage, error) {
	l := len(data)
	if len(m) < ControlMessageSpace(l) {
		return nil, errors.New("short message")
	}
	h := (*cmsghdr)(unsafe.Pointer(&m[0]))
	h.set(controlMessageLen(l), lvl, typ)
	if l > 0 {
		copy(m.Data(l), data)
	}
	return m.Next(l), nil
}

//
func (m ControlMessage) Parse() ([]ControlMessage, error) {
	var ms []ControlMessage
	for len(m) >= controlHeaderLen() {
		h := (*cmsghdr)(unsafe.Pointer(&m[0]))
		l := h.len()
		if l <= 0 {
			return nil, errors.New("invalid header length")
		}
		if uint64(l) < uint64(controlHeaderLen()) {
			return nil, errors.New("invalid message length")
		}
		if uint64(l) > uint64(len(m)) {
			return nil, errors.New("short buffer")
		}

		//

		//

		//

		ms = append(ms, ControlMessage(m[:l]))
		ll := l - controlHeaderLen()
		if len(m) >= ControlMessageSpace(ll) {
			m = m[ControlMessageSpace(ll):]
		} else {
			m = m[controlMessageLen(ll):]
		}
	}
	return ms, nil
}

func NewControlMessage(dataLen []int) ControlMessage {
	var l int
	for i := range dataLen {
		l += ControlMessageSpace(dataLen[i])
	}
	return make([]byte, l)
}

type Message struct {
	Buffers [][]byte

	OOB []byte

	Addr net.Addr

	N     int // # of bytes read or written from/to Buffers
	NN    int // # of bytes read or written from/to OOB
	Flags int // protocol-specific information on the received message
}

//
func (c *Conn) RecvMsg(m *Message, flags int) error {
	return c.recvMsg(m, flags)
}

//
func (c *Conn) SendMsg(m *Message, flags int) error {
	return c.sendMsg(m, flags)
}

//
//
//
func (c *Conn) RecvMsgs(ms []Message, flags int) (int, error) {
	return c.recvMsgs(ms, flags)
}

//
//
//
func (c *Conn) SendMsgs(ms []Message, flags int) (int, error) {
	return c.sendMsgs(ms, flags)
}
