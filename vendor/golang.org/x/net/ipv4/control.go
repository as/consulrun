// Copyright 2012 The Go Authors. All rights reserved.

package ipv4

import (
	"fmt"
	"net"
	"sync"

	"golang.org/x/net/internal/iana"
	"golang.org/x/net/internal/socket"
)

type rawOpt struct {
	sync.RWMutex
	cflags ControlFlags
}

func (c *rawOpt) set(f ControlFlags)        { c.cflags |= f }
func (c *rawOpt) clear(f ControlFlags)      { c.cflags &^= f }
func (c *rawOpt) isset(f ControlFlags) bool { return c.cflags&f != 0 }

type ControlFlags uint

const (
	FlagTTL       ControlFlags = 1 << iota // pass the TTL on the received packet
	FlagSrc                                // pass the source address on the received packet
	FlagDst                                // pass the destination address on the received packet
	FlagInterface                          // pass the interface index on the received packet
)

type ControlMessage struct {

	//

	//
	TTL     int    // time-to-live, receiving only
	Src     net.IP // source address, specifying only
	Dst     net.IP // destination address, receiving only
	IfIndex int    // interface index, must be 1 <= value when specifying
}

func (cm *ControlMessage) String() string {
	if cm == nil {
		return "<nil>"
	}
	return fmt.Sprintf("ttl=%d src=%v dst=%v ifindex=%d", cm.TTL, cm.Src, cm.Dst, cm.IfIndex)
}

func (cm *ControlMessage) Marshal() []byte {
	if cm == nil {
		return nil
	}
	var m socket.ControlMessage
	if ctlOpts[ctlPacketInfo].name > 0 && (cm.Src.To4() != nil || cm.IfIndex > 0) {
		m = socket.NewControlMessage([]int{ctlOpts[ctlPacketInfo].length})
	}
	if len(m) > 0 {
		ctlOpts[ctlPacketInfo].marshal(m, cm)
	}
	return m
}

func (cm *ControlMessage) Parse(b []byte) error {
	ms, err := socket.ControlMessage(b).Parse()
	if err != nil {
		return err
	}
	for _, m := range ms {
		lvl, typ, l, err := m.ParseHeader()
		if err != nil {
			return err
		}
		if lvl != iana.ProtocolIP {
			continue
		}
		switch {
		case typ == ctlOpts[ctlTTL].name && l >= ctlOpts[ctlTTL].length:
			ctlOpts[ctlTTL].parse(cm, m.Data(l))
		case typ == ctlOpts[ctlDst].name && l >= ctlOpts[ctlDst].length:
			ctlOpts[ctlDst].parse(cm, m.Data(l))
		case typ == ctlOpts[ctlInterface].name && l >= ctlOpts[ctlInterface].length:
			ctlOpts[ctlInterface].parse(cm, m.Data(l))
		case typ == ctlOpts[ctlPacketInfo].name && l >= ctlOpts[ctlPacketInfo].length:
			ctlOpts[ctlPacketInfo].parse(cm, m.Data(l))
		}
	}
	return nil
}

//
func NewControlMessage(cf ControlFlags) []byte {
	opt := rawOpt{cflags: cf}
	var l int
	if opt.isset(FlagTTL) && ctlOpts[ctlTTL].name > 0 {
		l += socket.ControlMessageSpace(ctlOpts[ctlTTL].length)
	}
	if ctlOpts[ctlPacketInfo].name > 0 {
		if opt.isset(FlagSrc | FlagDst | FlagInterface) {
			l += socket.ControlMessageSpace(ctlOpts[ctlPacketInfo].length)
		}
	} else {
		if opt.isset(FlagDst) && ctlOpts[ctlDst].name > 0 {
			l += socket.ControlMessageSpace(ctlOpts[ctlDst].length)
		}
		if opt.isset(FlagInterface) && ctlOpts[ctlInterface].name > 0 {
			l += socket.ControlMessageSpace(ctlOpts[ctlInterface].length)
		}
	}
	var b []byte
	if l > 0 {
		b = make([]byte, l)
	}
	return b
}

const (
	ctlTTL        = iota // header field
	ctlSrc               // header field
	ctlDst               // header field
	ctlInterface         // inbound or outbound interface
	ctlPacketInfo        // inbound or outbound packet path
	ctlMax
)

type ctlOpt struct {
	name    int // option name, must be equal or greater than 1
	length  int // option length
	marshal func([]byte, *ControlMessage) []byte
	parse   func(*ControlMessage, []byte)
}
