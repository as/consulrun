package dns

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
)

const (
	EDNS0LLQ          = 0x1     // long lived queries: http://tools.ietf.org/html/draft-sekar-dns-llq-01
	EDNS0UL           = 0x2     // update lease draft: http://files.dns-sd.org/draft-sekar-dns-ul.txt
	EDNS0NSID         = 0x3     // nsid (See RFC 5001)
	EDNS0DAU          = 0x5     // DNSSEC Algorithm Understood
	EDNS0DHU          = 0x6     // DS Hash Understood
	EDNS0N3U          = 0x7     // NSEC3 Hash Understood
	EDNS0SUBNET       = 0x8     // client-subnet (See RFC 7871)
	EDNS0EXPIRE       = 0x9     // EDNS0 expire
	EDNS0COOKIE       = 0xa     // EDNS0 Cookie
	EDNS0TCPKEEPALIVE = 0xb     // EDNS0 tcp keep alive (See RFC 7828)
	EDNS0PADDING      = 0xc     // EDNS0 padding (See RFC 7830)
	EDNS0LOCALSTART   = 0xFDE9  // Beginning of range reserved for local/experimental use (See RFC 6891)
	EDNS0LOCALEND     = 0xFFFE  // End of range reserved for local/experimental use (See RFC 6891)
	_DO               = 1 << 15 // DNSSEC OK
)

type OPT struct {
	Hdr    RR_Header
	Option []EDNS0 `dns:"opt"`
}

func (rr *OPT) String() string {
	s := "\n;; OPT PSEUDOSECTION:\n; EDNS: version " + strconv.Itoa(int(rr.Version())) + "; "
	if rr.Do() {
		s += "flags: do; "
	} else {
		s += "flags: ; "
	}
	s += "udp: " + strconv.Itoa(int(rr.UDPSize()))

	for _, o := range rr.Option {
		switch o.(type) {
		case *EDNS0_NSID:
			s += "\n; NSID: " + o.String()
			h, e := o.pack()
			var r string
			if e == nil {
				for _, c := range h {
					r += "(" + string(c) + ")"
				}
				s += "  " + r
			}
		case *EDNS0_SUBNET:
			s += "\n; SUBNET: " + o.String()
		case *EDNS0_COOKIE:
			s += "\n; COOKIE: " + o.String()
		case *EDNS0_UL:
			s += "\n; UPDATE LEASE: " + o.String()
		case *EDNS0_LLQ:
			s += "\n; LONG LIVED QUERIES: " + o.String()
		case *EDNS0_DAU:
			s += "\n; DNSSEC ALGORITHM UNDERSTOOD: " + o.String()
		case *EDNS0_DHU:
			s += "\n; DS HASH UNDERSTOOD: " + o.String()
		case *EDNS0_N3U:
			s += "\n; NSEC3 HASH UNDERSTOOD: " + o.String()
		case *EDNS0_LOCAL:
			s += "\n; LOCAL OPT: " + o.String()
		case *EDNS0_PADDING:
			s += "\n; PADDING: " + o.String()
		}
	}
	return s
}

func (rr *OPT) len() int {
	l := rr.Hdr.len()
	for i := 0; i < len(rr.Option); i++ {
		l += 4 // Account for 2-byte option code and 2-byte option length.
		lo, _ := rr.Option[i].pack()
		l += len(lo)
	}
	return l
}

func (rr *OPT) Version() uint8 {
	return uint8((rr.Hdr.Ttl & 0x00FF0000) >> 16)
}

func (rr *OPT) SetVersion(v uint8) {
	rr.Hdr.Ttl = rr.Hdr.Ttl&0xFF00FFFF | (uint32(v) << 16)
}

func (rr *OPT) ExtendedRcode() int {
	return int((rr.Hdr.Ttl & 0xFF000000) >> 24)
}

func (rr *OPT) SetExtendedRcode(v uint8) {
	rr.Hdr.Ttl = rr.Hdr.Ttl&0x00FFFFFF | (uint32(v) << 24)
}

func (rr *OPT) UDPSize() uint16 {
	return rr.Hdr.Class
}

func (rr *OPT) SetUDPSize(size uint16) {
	rr.Hdr.Class = size
}

func (rr *OPT) Do() bool {
	return rr.Hdr.Ttl&_DO == _DO
}

func (rr *OPT) SetDo(do ...bool) {
	if len(do) == 1 {
		if do[0] {
			rr.Hdr.Ttl |= _DO
		} else {
			rr.Hdr.Ttl &^= _DO
		}
	} else {
		rr.Hdr.Ttl |= _DO
	}
}

type EDNS0 interface {
	Option() uint16

	pack() ([]byte, error)

	unpack([]byte) error

	String() string
}

//
type EDNS0_NSID struct {
	Code uint16 // Always EDNS0NSID
	Nsid string // This string needs to be hex encoded
}

func (e *EDNS0_NSID) pack() ([]byte, error) {
	h, err := hex.DecodeString(e.Nsid)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (e *EDNS0_NSID) Option() uint16        { return EDNS0NSID } // Option returns the option code.
func (e *EDNS0_NSID) unpack(b []byte) error { e.Nsid = hex.EncodeToString(b); return nil }
func (e *EDNS0_NSID) String() string        { return string(e.Nsid) }

//
//
type EDNS0_SUBNET struct {
	Code          uint16 // Always EDNS0SUBNET
	Family        uint16 // 1 for IP, 2 for IP6
	SourceNetmask uint8
	SourceScope   uint8
	Address       net.IP
}

func (e *EDNS0_SUBNET) Option() uint16 { return EDNS0SUBNET }

func (e *EDNS0_SUBNET) pack() ([]byte, error) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint16(b[0:], e.Family)
	b[2] = e.SourceNetmask
	b[3] = e.SourceScope
	switch e.Family {
	case 0:

		if e.SourceNetmask != 0 {
			return nil, errors.New("dns: bad address family")
		}
	case 1:
		if e.SourceNetmask > net.IPv4len*8 {
			return nil, errors.New("dns: bad netmask")
		}
		if len(e.Address.To4()) != net.IPv4len {
			return nil, errors.New("dns: bad address")
		}
		ip := e.Address.To4().Mask(net.CIDRMask(int(e.SourceNetmask), net.IPv4len*8))
		needLength := (e.SourceNetmask + 8 - 1) / 8 // division rounding up
		b = append(b, ip[:needLength]...)
	case 2:
		if e.SourceNetmask > net.IPv6len*8 {
			return nil, errors.New("dns: bad netmask")
		}
		if len(e.Address) != net.IPv6len {
			return nil, errors.New("dns: bad address")
		}
		ip := e.Address.Mask(net.CIDRMask(int(e.SourceNetmask), net.IPv6len*8))
		needLength := (e.SourceNetmask + 8 - 1) / 8 // division rounding up
		b = append(b, ip[:needLength]...)
	default:
		return nil, errors.New("dns: bad address family")
	}
	return b, nil
}

func (e *EDNS0_SUBNET) unpack(b []byte) error {
	if len(b) < 4 {
		return ErrBuf
	}
	e.Family = binary.BigEndian.Uint16(b)
	e.SourceNetmask = b[2]
	e.SourceScope = b[3]
	switch e.Family {
	case 0:

		if e.SourceNetmask != 0 {
			return errors.New("dns: bad address family")
		}
		e.Address = net.IPv4(0, 0, 0, 0)
	case 1:
		if e.SourceNetmask > net.IPv4len*8 || e.SourceScope > net.IPv4len*8 {
			return errors.New("dns: bad netmask")
		}
		addr := make([]byte, net.IPv4len)
		for i := 0; i < net.IPv4len && 4+i < len(b); i++ {
			addr[i] = b[4+i]
		}
		e.Address = net.IPv4(addr[0], addr[1], addr[2], addr[3])
	case 2:
		if e.SourceNetmask > net.IPv6len*8 || e.SourceScope > net.IPv6len*8 {
			return errors.New("dns: bad netmask")
		}
		addr := make([]byte, net.IPv6len)
		for i := 0; i < net.IPv6len && 4+i < len(b); i++ {
			addr[i] = b[4+i]
		}
		e.Address = net.IP{addr[0], addr[1], addr[2], addr[3], addr[4],
			addr[5], addr[6], addr[7], addr[8], addr[9], addr[10],
			addr[11], addr[12], addr[13], addr[14], addr[15]}
	default:
		return errors.New("dns: bad address family")
	}
	return nil
}

func (e *EDNS0_SUBNET) String() (s string) {
	if e.Address == nil {
		s = "<nil>"
	} else if e.Address.To4() != nil {
		s = e.Address.String()
	} else {
		s = "[" + e.Address.String() + "]"
	}
	s += "/" + strconv.Itoa(int(e.SourceNetmask)) + "/" + strconv.Itoa(int(e.SourceScope))
	return
}

//
//
//
//
type EDNS0_COOKIE struct {
	Code   uint16 // Always EDNS0COOKIE
	Cookie string // Hex-encoded cookie data
}

func (e *EDNS0_COOKIE) pack() ([]byte, error) {
	h, err := hex.DecodeString(e.Cookie)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (e *EDNS0_COOKIE) Option() uint16        { return EDNS0COOKIE }
func (e *EDNS0_COOKIE) unpack(b []byte) error { e.Cookie = hex.EncodeToString(b); return nil }
func (e *EDNS0_COOKIE) String() string        { return e.Cookie }

//
type EDNS0_UL struct {
	Code  uint16 // Always EDNS0UL
	Lease uint32
}

func (e *EDNS0_UL) Option() uint16 { return EDNS0UL }
func (e *EDNS0_UL) String() string { return strconv.FormatUint(uint64(e.Lease), 10) }

func (e *EDNS0_UL) pack() ([]byte, error) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, e.Lease)
	return b, nil
}

func (e *EDNS0_UL) unpack(b []byte) error {
	if len(b) < 4 {
		return ErrBuf
	}
	e.Lease = binary.BigEndian.Uint32(b)
	return nil
}

type EDNS0_LLQ struct {
	Code      uint16 // Always EDNS0LLQ
	Version   uint16
	Opcode    uint16
	Error     uint16
	Id        uint64
	LeaseLife uint32
}

func (e *EDNS0_LLQ) Option() uint16 { return EDNS0LLQ }

func (e *EDNS0_LLQ) pack() ([]byte, error) {
	b := make([]byte, 18)
	binary.BigEndian.PutUint16(b[0:], e.Version)
	binary.BigEndian.PutUint16(b[2:], e.Opcode)
	binary.BigEndian.PutUint16(b[4:], e.Error)
	binary.BigEndian.PutUint64(b[6:], e.Id)
	binary.BigEndian.PutUint32(b[14:], e.LeaseLife)
	return b, nil
}

func (e *EDNS0_LLQ) unpack(b []byte) error {
	if len(b) < 18 {
		return ErrBuf
	}
	e.Version = binary.BigEndian.Uint16(b[0:])
	e.Opcode = binary.BigEndian.Uint16(b[2:])
	e.Error = binary.BigEndian.Uint16(b[4:])
	e.Id = binary.BigEndian.Uint64(b[6:])
	e.LeaseLife = binary.BigEndian.Uint32(b[14:])
	return nil
}

func (e *EDNS0_LLQ) String() string {
	s := strconv.FormatUint(uint64(e.Version), 10) + " " + strconv.FormatUint(uint64(e.Opcode), 10) +
		" " + strconv.FormatUint(uint64(e.Error), 10) + " " + strconv.FormatUint(uint64(e.Id), 10) +
		" " + strconv.FormatUint(uint64(e.LeaseLife), 10)
	return s
}

type EDNS0_DAU struct {
	Code    uint16 // Always EDNS0DAU
	AlgCode []uint8
}

func (e *EDNS0_DAU) Option() uint16        { return EDNS0DAU }
func (e *EDNS0_DAU) pack() ([]byte, error) { return e.AlgCode, nil }
func (e *EDNS0_DAU) unpack(b []byte) error { e.AlgCode = b; return nil }

func (e *EDNS0_DAU) String() string {
	s := ""
	for i := 0; i < len(e.AlgCode); i++ {
		if a, ok := AlgorithmToString[e.AlgCode[i]]; ok {
			s += " " + a
		} else {
			s += " " + strconv.Itoa(int(e.AlgCode[i]))
		}
	}
	return s
}

type EDNS0_DHU struct {
	Code    uint16 // Always EDNS0DHU
	AlgCode []uint8
}

func (e *EDNS0_DHU) Option() uint16        { return EDNS0DHU }
func (e *EDNS0_DHU) pack() ([]byte, error) { return e.AlgCode, nil }
func (e *EDNS0_DHU) unpack(b []byte) error { e.AlgCode = b; return nil }

func (e *EDNS0_DHU) String() string {
	s := ""
	for i := 0; i < len(e.AlgCode); i++ {
		if a, ok := HashToString[e.AlgCode[i]]; ok {
			s += " " + a
		} else {
			s += " " + strconv.Itoa(int(e.AlgCode[i]))
		}
	}
	return s
}

type EDNS0_N3U struct {
	Code    uint16 // Always EDNS0N3U
	AlgCode []uint8
}

func (e *EDNS0_N3U) Option() uint16        { return EDNS0N3U }
func (e *EDNS0_N3U) pack() ([]byte, error) { return e.AlgCode, nil }
func (e *EDNS0_N3U) unpack(b []byte) error { e.AlgCode = b; return nil }

func (e *EDNS0_N3U) String() string {

	s := ""
	for i := 0; i < len(e.AlgCode); i++ {
		if a, ok := HashToString[e.AlgCode[i]]; ok {
			s += " " + a
		} else {
			s += " " + strconv.Itoa(int(e.AlgCode[i]))
		}
	}
	return s
}

type EDNS0_EXPIRE struct {
	Code   uint16 // Always EDNS0EXPIRE
	Expire uint32
}

func (e *EDNS0_EXPIRE) Option() uint16 { return EDNS0EXPIRE }
func (e *EDNS0_EXPIRE) String() string { return strconv.FormatUint(uint64(e.Expire), 10) }

func (e *EDNS0_EXPIRE) pack() ([]byte, error) {
	b := make([]byte, 4)
	b[0] = byte(e.Expire >> 24)
	b[1] = byte(e.Expire >> 16)
	b[2] = byte(e.Expire >> 8)
	b[3] = byte(e.Expire)
	return b, nil
}

func (e *EDNS0_EXPIRE) unpack(b []byte) error {
	if len(b) < 4 {
		return ErrBuf
	}
	e.Expire = binary.BigEndian.Uint32(b)
	return nil
}

//
type EDNS0_LOCAL struct {
	Code uint16
	Data []byte
}

func (e *EDNS0_LOCAL) Option() uint16 { return e.Code }
func (e *EDNS0_LOCAL) String() string {
	return strconv.FormatInt(int64(e.Code), 10) + ":0x" + hex.EncodeToString(e.Data)
}

func (e *EDNS0_LOCAL) pack() ([]byte, error) {
	b := make([]byte, len(e.Data))
	copied := copy(b, e.Data)
	if copied != len(e.Data) {
		return nil, ErrBuf
	}
	return b, nil
}

func (e *EDNS0_LOCAL) unpack(b []byte) error {
	e.Data = make([]byte, len(b))
	copied := copy(e.Data, b)
	if copied != len(b) {
		return ErrBuf
	}
	return nil
}

type EDNS0_TCP_KEEPALIVE struct {
	Code    uint16 // Always EDNSTCPKEEPALIVE
	Length  uint16 // the value 0 if the TIMEOUT is omitted, the value 2 if it is present;
	Timeout uint16 // an idle timeout value for the TCP connection, specified in units of 100 milliseconds, encoded in network byte order.
}

func (e *EDNS0_TCP_KEEPALIVE) Option() uint16 { return EDNS0TCPKEEPALIVE }

func (e *EDNS0_TCP_KEEPALIVE) pack() ([]byte, error) {
	if e.Timeout != 0 && e.Length != 2 {
		return nil, errors.New("dns: timeout specified but length is not 2")
	}
	if e.Timeout == 0 && e.Length != 0 {
		return nil, errors.New("dns: timeout not specified but length is not 0")
	}
	b := make([]byte, 4+e.Length)
	binary.BigEndian.PutUint16(b[0:], e.Code)
	binary.BigEndian.PutUint16(b[2:], e.Length)
	if e.Length == 2 {
		binary.BigEndian.PutUint16(b[4:], e.Timeout)
	}
	return b, nil
}

func (e *EDNS0_TCP_KEEPALIVE) unpack(b []byte) error {
	if len(b) < 4 {
		return ErrBuf
	}
	e.Length = binary.BigEndian.Uint16(b[2:4])
	if e.Length != 0 && e.Length != 2 {
		return errors.New("dns: length mismatch, want 0/2 but got " + strconv.FormatUint(uint64(e.Length), 10))
	}
	if e.Length == 2 {
		if len(b) < 6 {
			return ErrBuf
		}
		e.Timeout = binary.BigEndian.Uint16(b[4:6])
	}
	return nil
}

func (e *EDNS0_TCP_KEEPALIVE) String() (s string) {
	s = "use tcp keep-alive"
	if e.Length == 0 {
		s += ", timeout omitted"
	} else {
		s += fmt.Sprintf(", timeout %dms", e.Timeout*100)
	}
	return
}

type EDNS0_PADDING struct {
	Padding []byte
}

func (e *EDNS0_PADDING) Option() uint16        { return EDNS0PADDING }
func (e *EDNS0_PADDING) pack() ([]byte, error) { return e.Padding, nil }
func (e *EDNS0_PADDING) unpack(b []byte) error { e.Padding = b; return nil }
func (e *EDNS0_PADDING) String() string        { return fmt.Sprintf("%0X", e.Padding) }
