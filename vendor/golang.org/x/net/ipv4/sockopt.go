// Copyright 2014 The Go Authors. All rights reserved.

package ipv4

import "golang.org/x/net/internal/socket"

const (
	ssoTOS                = iota // header field for unicast packet
	ssoTTL                       // header field for unicast packet
	ssoMulticastTTL              // header field for multicast packet
	ssoMulticastInterface        // outbound interface for multicast packet
	ssoMulticastLoopback         // loopback for multicast packet
	ssoReceiveTTL                // header field on received packet
	ssoReceiveDst                // header field on received packet
	ssoReceiveInterface          // inbound interface on received packet
	ssoPacketInfo                // incbound or outbound packet path
	ssoHeaderPrepend             // ipv4 header prepend
	ssoStripHeader               // strip ipv4 header
	ssoICMPFilter                // icmp filter
	ssoJoinGroup                 // any-source multicast
	ssoLeaveGroup                // any-source multicast
	ssoJoinSourceGroup           // source-specific multicast
	ssoLeaveSourceGroup          // source-specific multicast
	ssoBlockSourceGroup          // any-source or source-specific multicast
	ssoUnblockSourceGroup        // any-source or source-specific multicast
	ssoAttachFilter              // attach BPF for filtering inbound traffic
)

const (
	ssoTypeIPMreq = iota + 1
	ssoTypeIPMreqn
	ssoTypeGroupReq
	ssoTypeGroupSourceReq
)

type sockOpt struct {
	socket.Option
	typ int // hint for option value type; optional
}
