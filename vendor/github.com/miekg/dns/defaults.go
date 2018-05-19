package dns

import (
	"errors"
	"net"
	"strconv"
)

const hexDigit = "0123456789abcdef"

func (dns *Msg) SetReply(request *Msg) *Msg {
	dns.Id = request.Id
	dns.Response = true
	dns.Opcode = request.Opcode
	if dns.Opcode == OpcodeQuery {
		dns.RecursionDesired = request.RecursionDesired // Copy rd bit
		dns.CheckingDisabled = request.CheckingDisabled // Copy cd bit
	}
	dns.Rcode = RcodeSuccess
	if len(request.Question) > 0 {
		dns.Question = make([]Question, 1)
		dns.Question[0] = request.Question[0]
	}
	return dns
}

func (dns *Msg) SetQuestion(z string, t uint16) *Msg {
	dns.Id = Id()
	dns.RecursionDesired = true
	dns.Question = make([]Question, 1)
	dns.Question[0] = Question{z, t, ClassINET}
	return dns
}

func (dns *Msg) SetNotify(z string) *Msg {
	dns.Opcode = OpcodeNotify
	dns.Authoritative = true
	dns.Id = Id()
	dns.Question = make([]Question, 1)
	dns.Question[0] = Question{z, TypeSOA, ClassINET}
	return dns
}

func (dns *Msg) SetRcode(request *Msg, rcode int) *Msg {
	dns.SetReply(request)
	dns.Rcode = rcode
	return dns
}

func (dns *Msg) SetRcodeFormatError(request *Msg) *Msg {
	dns.Rcode = RcodeFormatError
	dns.Opcode = OpcodeQuery
	dns.Response = true
	dns.Authoritative = false
	dns.Id = request.Id
	return dns
}

func (dns *Msg) SetUpdate(z string) *Msg {
	dns.Id = Id()
	dns.Response = false
	dns.Opcode = OpcodeUpdate
	dns.Compress = false // BIND9 cannot handle compression
	dns.Question = make([]Question, 1)
	dns.Question[0] = Question{z, TypeSOA, ClassINET}
	return dns
}

func (dns *Msg) SetIxfr(z string, serial uint32, ns, mbox string) *Msg {
	dns.Id = Id()
	dns.Question = make([]Question, 1)
	dns.Ns = make([]RR, 1)
	s := new(SOA)
	s.Hdr = RR_Header{z, TypeSOA, ClassINET, defaultTtl, 0}
	s.Serial = serial
	s.Ns = ns
	s.Mbox = mbox
	dns.Question[0] = Question{z, TypeIXFR, ClassINET}
	dns.Ns[0] = s
	return dns
}

func (dns *Msg) SetAxfr(z string) *Msg {
	dns.Id = Id()
	dns.Question = make([]Question, 1)
	dns.Question[0] = Question{z, TypeAXFR, ClassINET}
	return dns
}

func (dns *Msg) SetTsig(z, algo string, fudge uint16, timesigned int64) *Msg {
	t := new(TSIG)
	t.Hdr = RR_Header{z, TypeTSIG, ClassANY, 0, 0}
	t.Algorithm = algo
	t.Fudge = fudge
	t.TimeSigned = uint64(timesigned)
	t.OrigId = dns.Id
	dns.Extra = append(dns.Extra, t)
	return dns
}

func (dns *Msg) SetEdns0(udpsize uint16, do bool) *Msg {
	e := new(OPT)
	e.Hdr.Name = "."
	e.Hdr.Rrtype = TypeOPT
	e.SetUDPSize(udpsize)
	if do {
		e.SetDo()
	}
	dns.Extra = append(dns.Extra, e)
	return dns
}

func (dns *Msg) IsTsig() *TSIG {
	if len(dns.Extra) > 0 {
		if dns.Extra[len(dns.Extra)-1].Header().Rrtype == TypeTSIG {
			return dns.Extra[len(dns.Extra)-1].(*TSIG)
		}
	}
	return nil
}

func (dns *Msg) IsEdns0() *OPT {

	for i := len(dns.Extra) - 1; i >= 0; i-- {
		if dns.Extra[i].Header().Rrtype == TypeOPT {
			return dns.Extra[i].(*OPT)
		}
	}
	return nil
}

func IsDomainName(s string) (labels int, ok bool) {
	_, labels, err := packDomainName(s, nil, 0, nil, false)
	return labels, err == nil
}

func IsSubDomain(parent, child string) bool {

	return CompareDomainName(parent, child) == CountLabel(parent)
}

func IsMsg(buf []byte) error {

	if len(buf) < 12 {
		return errors.New("dns: bad message header")
	}

	return nil
}

func IsFqdn(s string) bool {
	l := len(s)
	if l == 0 {
		return false
	}
	return s[l-1] == '.'
}

func IsRRset(rrset []RR) bool {
	if len(rrset) == 0 {
		return false
	}
	if len(rrset) == 1 {
		return true
	}
	rrHeader := rrset[0].Header()
	rrType := rrHeader.Rrtype
	rrClass := rrHeader.Class
	rrName := rrHeader.Name

	for _, rr := range rrset[1:] {
		curRRHeader := rr.Header()
		if curRRHeader.Rrtype != rrType || curRRHeader.Class != rrClass || curRRHeader.Name != rrName {

			return false
		}
	}

	return true
}

func Fqdn(s string) string {
	if IsFqdn(s) {
		return s
	}
	return s + "."
}

func ReverseAddr(addr string) (arpa string, err error) {
	ip := net.ParseIP(addr)
	if ip == nil {
		return "", &Error{err: "unrecognized address: " + addr}
	}
	if ip.To4() != nil {
		return strconv.Itoa(int(ip[15])) + "." + strconv.Itoa(int(ip[14])) + "." + strconv.Itoa(int(ip[13])) + "." +
			strconv.Itoa(int(ip[12])) + ".in-addr.arpa.", nil
	}

	buf := make([]byte, 0, len(ip)*4+len("ip6.arpa."))

	for i := len(ip) - 1; i >= 0; i-- {
		v := ip[i]
		buf = append(buf, hexDigit[v&0xF])
		buf = append(buf, '.')
		buf = append(buf, hexDigit[v>>4])
		buf = append(buf, '.')
	}

	buf = append(buf, "ip6.arpa."...)
	return string(buf), nil
}

func (t Type) String() string {
	if t1, ok := TypeToString[uint16(t)]; ok {
		return t1
	}
	return "TYPE" + strconv.Itoa(int(t))
}

func (c Class) String() string {
	if s, ok := ClassToString[uint16(c)]; ok {

		if _, ok := StringToType[s]; !ok {
			return s
		}
	}
	return "CLASS" + strconv.Itoa(int(c))
}

func (n Name) String() string {
	return sprintName(string(n))
}
