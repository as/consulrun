// Copyright 2013 The Go Authors. All rights reserved.

package ipv6

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
	FlagTrafficClass ControlFlags = 1 << iota // pass the traffic class on the received packet
	FlagHopLimit                              // pass the hop limit on the received packet
	FlagSrc                                   // pass the source address on the received packet
	FlagDst                                   // pass the destination address on the received packet
	FlagInterface                             // pass the interface index on the received packet
	FlagPathMTU                               // pass the path MTU on the received packet path
)

const flagPacketInfo = FlagDst | FlagInterface

type ControlMessage struct {

	//

	//
	TrafficClass int    // traffic class, must be 1 <= value <= 255 when specifying
	HopLimit     int    // hop limit, must be 1 <= value <= 255 when specifying
	Src          net.IP // source address, specifying only
	Dst          net.IP // destination address, receiving only
	IfIndex      int    // interface index, must be 1 <= value when specifying
	NextHop      net.IP // next hop address, specifying only
	MTU          int    // path MTU, receiving only
}

func (cm *ControlMessage) String() string {
	if cm == nil {
		return "<nil>"
	}
	return fmt.Sprintf("tclass=%#x hoplim=%d src=%v dst=%v ifindex=%d nexthop=%v mtu=%d", cm.TrafficClass, cm.HopLimit, cm.Src, cm.Dst, cm.IfIndex, cm.NextHop, cm.MTU)
}

func (cm *ControlMessage) Marshal() []byte {
	if cm == nil {
		return nil
	}
	var l int
	tclass := false
	if ctlOpts[ctlTrafficClass].name > 0 && cm.TrafficClass > 0 {
		tclass = true
		l += socket.ControlMessageSpace(ctlOpts[ctlTrafficClass].length)
	}
	hoplimit := false
	if ctlOpts[ctlHopLimit].name > 0 && cm.HopLimit > 0 {
		hoplimit = true
		l += socket.ControlMessageSpace(ctlOpts[ctlHopLimit].length)
	}
	pktinfo := false
	if ctlOpts[ctlPacketInfo].name > 0 && (cm.Src.To16() != nil && cm.Src.To4() == nil || cm.IfIndex > 0) {
		pktinfo = true
		l += socket.ControlMessageSpace(ctlOpts[ctlPacketInfo].length)
	}
	nexthop := false
	if ctlOpts[ctlNextHop].name > 0 && cm.NextHop.To16() != nil && cm.NextHop.To4() == nil {
		nexthop = true
		l += socket.ControlMessageSpace(ctlOpts[ctlNextHop].length)
	}
	var b []byte
	if l > 0 {
		b = make([]byte, l)
		bb := b
		if tclass {
			bb = ctlOpts[ctlTrafficClass].marshal(bb, cm)
		}
		if hoplimit {
			bb = ctlOpts[ctlHopLimit].marshal(bb, cm)
		}
		if pktinfo {
			bb = ctlOpts[ctlPacketInfo].marshal(bb, cm)
		}
		if nexthop {
			bb = ctlOpts[ctlNextHop].marshal(bb, cm)
		}
	}
	return b
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
		if lvl != iana.ProtocolIPv6 {
			continue
		}
		switch {
		case typ == ctlOpts[ctlTrafficClass].name && l >= ctlOpts[ctlTrafficClass].length:
			ctlOpts[ctlTrafficClass].parse(cm, m.Data(l))
		case typ == ctlOpts[ctlHopLimit].name && l >= ctlOpts[ctlHopLimit].length:
			ctlOpts[ctlHopLimit].parse(cm, m.Data(l))
		case typ == ctlOpts[ctlPacketInfo].name && l >= ctlOpts[ctlPacketInfo].length:
			ctlOpts[ctlPacketInfo].parse(cm, m.Data(l))
		case typ == ctlOpts[ctlPathMTU].name && l >= ctlOpts[ctlPathMTU].length:
			ctlOpts[ctlPathMTU].parse(cm, m.Data(l))
		}
	}
	return nil
}

//
func NewControlMessage(cf ControlFlags) []byte {
	opt := rawOpt{cflags: cf}
	var l int
	if opt.isset(FlagTrafficClass) && ctlOpts[ctlTrafficClass].name > 0 {
		l += socket.ControlMessageSpace(ctlOpts[ctlTrafficClass].length)
	}
	if opt.isset(FlagHopLimit) && ctlOpts[ctlHopLimit].name > 0 {
		l += socket.ControlMessageSpace(ctlOpts[ctlHopLimit].length)
	}
	if opt.isset(flagPacketInfo) && ctlOpts[ctlPacketInfo].name > 0 {
		l += socket.ControlMessageSpace(ctlOpts[ctlPacketInfo].length)
	}
	if opt.isset(FlagPathMTU) && ctlOpts[ctlPathMTU].name > 0 {
		l += socket.ControlMessageSpace(ctlOpts[ctlPathMTU].length)
	}
	var b []byte
	if l > 0 {
		b = make([]byte, l)
	}
	return b
}

const (
	ctlTrafficClass = iota // header field
	ctlHopLimit            // header field
	ctlPacketInfo          // inbound or outbound packet path
	ctlNextHop             // nexthop
	ctlPathMTU             // path mtu
	ctlMax
)

type ctlOpt struct {
	name    int // option name, must be equal or greater than 1
	length  int // option length
	marshal func([]byte, *ControlMessage) []byte
	parse   func(*ControlMessage, []byte)
}
