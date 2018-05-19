package dns

import (
	"fmt"
	"strings"
)

type PrivateRdata interface {
	String() string

	Parse([]string) error

	Pack([]byte) (int, error)

	Unpack([]byte) (int, error)

	Copy(PrivateRdata) error

	Len() int
}

type PrivateRR struct {
	Hdr  RR_Header
	Data PrivateRdata
}

func mkPrivateRR(rrtype uint16) *PrivateRR {

	rrfunc, ok := TypeToRR[rrtype]
	if !ok {
		panic(fmt.Sprintf("dns: invalid operation with Private RR type %d", rrtype))
	}

	anyrr := rrfunc()
	switch rr := anyrr.(type) {
	case *PrivateRR:
		return rr
	}
	panic(fmt.Sprintf("dns: RR is not a PrivateRR, TypeToRR[%d] generator returned %T", rrtype, anyrr))
}

func (r *PrivateRR) Header() *RR_Header { return &r.Hdr }

func (r *PrivateRR) String() string { return r.Hdr.String() + r.Data.String() }

func (r *PrivateRR) len() int { return r.Hdr.len() + r.Data.Len() }
func (r *PrivateRR) copy() RR {

	rr := mkPrivateRR(r.Hdr.Rrtype)
	newh := r.Hdr.copyHeader()
	rr.Hdr = *newh

	err := r.Data.Copy(rr.Data)
	if err != nil {
		panic("dns: got value that could not be used to copy Private rdata")
	}
	return rr
}
func (r *PrivateRR) pack(msg []byte, off int, compression map[string]int, compress bool) (int, error) {
	off, err := r.Hdr.pack(msg, off, compression, compress)
	if err != nil {
		return off, err
	}
	headerEnd := off
	n, err := r.Data.Pack(msg[off:])
	if err != nil {
		return len(msg), err
	}
	off += n
	r.Header().Rdlength = uint16(off - headerEnd)
	return off, nil
}

func PrivateHandle(rtypestr string, rtype uint16, generator func() PrivateRdata) {
	rtypestr = strings.ToUpper(rtypestr)

	TypeToRR[rtype] = func() RR { return &PrivateRR{RR_Header{}, generator()} }
	TypeToString[rtype] = rtypestr
	StringToType[rtypestr] = rtype

	typeToUnpack[rtype] = func(h RR_Header, msg []byte, off int) (RR, int, error) {
		if noRdata(h) {
			return &h, off, nil
		}
		var err error

		rr := mkPrivateRR(h.Rrtype)
		rr.Hdr = h

		off1, err := rr.Data.Unpack(msg[off:])
		off += off1
		if err != nil {
			return rr, off, err
		}
		return rr, off, err
	}

	setPrivateRR := func(h RR_Header, c chan lex, o, f string) (RR, *ParseError, string) {
		rr := mkPrivateRR(h.Rrtype)
		rr.Hdr = h

		var l lex
		text := make([]string, 0, 2) // could be 0..N elements, median is probably 1
	Fetch:
		for {

			switch l = <-c; l.value {
			case zNewline, zEOF:
				break Fetch
			case zString:
				text = append(text, l.token)
			}
		}

		err := rr.Data.Parse(text)
		if err != nil {
			return nil, &ParseError{f, err.Error(), l}, ""
		}

		return rr, nil, ""
	}

	typeToparserFunc[rtype] = parserFunc{setPrivateRR, true}
}

func PrivateHandleRemove(rtype uint16) {
	rtypestr, ok := TypeToString[rtype]
	if ok {
		delete(TypeToRR, rtype)
		delete(TypeToString, rtype)
		delete(typeToparserFunc, rtype)
		delete(StringToType, rtypestr)
		delete(typeToUnpack, rtype)
	}
	return
}
