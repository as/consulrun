// Copyright 2014 The Go Authors. All rights reserved.

package ipv6

import "golang.org/x/net/internal/socket"

const (
	ssoTrafficClass        = iota // header field for unicast packet, RFC 3542
	ssoHopLimit                   // header field for unicast packet, RFC 3493
	ssoMulticastInterface         // outbound interface for multicast packet, RFC 3493
	ssoMulticastHopLimit          // header field for multicast packet, RFC 3493
	ssoMulticastLoopback          // loopback for multicast packet, RFC 3493
	ssoReceiveTrafficClass        // header field on received packet, RFC 3542
	ssoReceiveHopLimit            // header field on received packet, RFC 2292 or 3542
	ssoReceivePacketInfo          // incbound or outbound packet path, RFC 2292 or 3542
	ssoReceivePathMTU             // path mtu, RFC 3542
	ssoPathMTU                    // path mtu, RFC 3542
	ssoChecksum                   // packet checksum, RFC 2292 or 3542
	ssoICMPFilter                 // icmp filter, RFC 2292 or 3542
	ssoJoinGroup                  // any-source multicast, RFC 3493
	ssoLeaveGroup                 // any-source multicast, RFC 3493
	ssoJoinSourceGroup            // source-specific multicast
	ssoLeaveSourceGroup           // source-specific multicast
	ssoBlockSourceGroup           // any-source or source-specific multicast
	ssoUnblockSourceGroup         // any-source or source-specific multicast
	ssoAttachFilter               // attach BPF for filtering inbound traffic
)

const (
	ssoTypeIPMreq = iota + 1
	ssoTypeGroupReq
	ssoTypeGroupSourceReq
)

type sockOpt struct {
	socket.Option
	typ int // hint for option value type; optional
}
