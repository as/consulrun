package dns

import "strconv"

const (
	year68     = 1 << 31 // For RFC1982 (Serial Arithmetic) calculations in 32 bits.
	defaultTtl = 3600    // Default internal TTL.

	DefaultMsgSize = 4096

	MinMsgSize = 512

	MaxMsgSize = 65535
)

type Error struct{ err string }

func (e *Error) Error() string {
	if e == nil {
		return "dns: <nil>"
	}
	return "dns: " + e.err
}

type RR interface {
	Header() *RR_Header

	String() string

	copy() RR

	len() int

	pack([]byte, int, map[string]int, bool) (int, error)
}

type RR_Header struct {
	Name     string `dns:"cdomain-name"`
	Rrtype   uint16
	Class    uint16
	Ttl      uint32
	Rdlength uint16 // Length of data after header.
}

func (h *RR_Header) Header() *RR_Header { return h }

func (h *RR_Header) copy() RR { return nil }

func (h *RR_Header) copyHeader() *RR_Header {
	r := new(RR_Header)
	r.Name = h.Name
	r.Rrtype = h.Rrtype
	r.Class = h.Class
	r.Ttl = h.Ttl
	r.Rdlength = h.Rdlength
	return r
}

func (h *RR_Header) String() string {
	var s string

	if h.Rrtype == TypeOPT {
		s = ";"

	}

	s += sprintName(h.Name) + "\t"
	s += strconv.FormatInt(int64(h.Ttl), 10) + "\t"
	s += Class(h.Class).String() + "\t"
	s += Type(h.Rrtype).String() + "\t"
	return s
}

func (h *RR_Header) len() int {
	l := len(h.Name) + 1
	l += 10 // rrtype(2) + class(2) + ttl(4) + rdlength(2)
	return l
}

func (rr *RFC3597) ToRFC3597(r RR) error {
	buf := make([]byte, r.len()*2)
	off, err := PackRR(r, buf, 0, nil, false)
	if err != nil {
		return err
	}
	buf = buf[:off]
	if int(r.Header().Rdlength) > off {
		return ErrBuf
	}

	rfc3597, _, err := unpackRFC3597(*r.Header(), buf, off-int(r.Header().Rdlength))
	if err != nil {
		return err
	}
	*rr = *rfc3597.(*RFC3597)
	return nil
}
