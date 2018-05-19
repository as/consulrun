// Copyright 2013 The Go Authors. All rights reserved.

package ipv4

import "golang.org/x/net/internal/iana"

type ICMPType int

func (typ ICMPType) String() string {
	s, ok := icmpTypes[typ]
	if !ok {
		return "<nil>"
	}
	return s
}

func (typ ICMPType) Protocol() int {
	return iana.ProtocolICMP
}

//
type ICMPFilter struct {
	icmpFilter
}

func (f *ICMPFilter) Accept(typ ICMPType) {
	f.accept(typ)
}

func (f *ICMPFilter) Block(typ ICMPType) {
	f.block(typ)
}

func (f *ICMPFilter) SetAll(block bool) {
	f.setAll(block)
}

func (f *ICMPFilter) WillBlock(typ ICMPType) bool {
	return f.willBlock(typ)
}
