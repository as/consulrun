package dns

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"strconv"
	"strings"
	"time"
)

const (
	HmacMD5    = "hmac-md5.sig-alg.reg.int."
	HmacSHA1   = "hmac-sha1."
	HmacSHA256 = "hmac-sha256."
	HmacSHA512 = "hmac-sha512."
)

type TSIG struct {
	Hdr        RR_Header
	Algorithm  string `dns:"domain-name"`
	TimeSigned uint64 `dns:"uint48"`
	Fudge      uint16
	MACSize    uint16
	MAC        string `dns:"size-hex:MACSize"`
	OrigId     uint16
	Error      uint16
	OtherLen   uint16
	OtherData  string `dns:"size-hex:OtherLen"`
}

func (rr *TSIG) String() string {
	s := "\n;; TSIG PSEUDOSECTION:\n"
	s += rr.Hdr.String() +
		" " + rr.Algorithm +
		" " + tsigTimeToString(rr.TimeSigned) +
		" " + strconv.Itoa(int(rr.Fudge)) +
		" " + strconv.Itoa(int(rr.MACSize)) +
		" " + strings.ToUpper(rr.MAC) +
		" " + strconv.Itoa(int(rr.OrigId)) +
		" " + strconv.Itoa(int(rr.Error)) + // BIND prints NOERROR
		" " + strconv.Itoa(int(rr.OtherLen)) +
		" " + rr.OtherData
	return s
}

type tsigWireFmt struct {
	Name  string `dns:"domain-name"`
	Class uint16
	Ttl   uint32

	Algorithm  string `dns:"domain-name"`
	TimeSigned uint64 `dns:"uint48"`
	Fudge      uint16

	Error     uint16
	OtherLen  uint16
	OtherData string `dns:"size-hex:OtherLen"`
}

type macWireFmt struct {
	MACSize uint16
	MAC     string `dns:"size-hex:MACSize"`
}

type timerWireFmt struct {
	TimeSigned uint64 `dns:"uint48"`
	Fudge      uint16
}

func TsigGenerate(m *Msg, secret, requestMAC string, timersOnly bool) ([]byte, string, error) {
	if m.IsTsig() == nil {
		panic("dns: TSIG not last RR in additional")
	}

	rawsecret, err := fromBase64([]byte(secret))
	if err != nil {
		return nil, "", err
	}

	rr := m.Extra[len(m.Extra)-1].(*TSIG)
	m.Extra = m.Extra[0 : len(m.Extra)-1] // kill the TSIG from the msg
	mbuf, err := m.Pack()
	if err != nil {
		return nil, "", err
	}
	buf := tsigBuffer(mbuf, rr, requestMAC, timersOnly)

	t := new(TSIG)
	var h hash.Hash
	switch strings.ToLower(rr.Algorithm) {
	case HmacMD5:
		h = hmac.New(md5.New, []byte(rawsecret))
	case HmacSHA1:
		h = hmac.New(sha1.New, []byte(rawsecret))
	case HmacSHA256:
		h = hmac.New(sha256.New, []byte(rawsecret))
	case HmacSHA512:
		h = hmac.New(sha512.New, []byte(rawsecret))
	default:
		return nil, "", ErrKeyAlg
	}
	h.Write(buf)
	t.MAC = hex.EncodeToString(h.Sum(nil))
	t.MACSize = uint16(len(t.MAC) / 2) // Size is half!

	t.Hdr = RR_Header{Name: rr.Hdr.Name, Rrtype: TypeTSIG, Class: ClassANY, Ttl: 0}
	t.Fudge = rr.Fudge
	t.TimeSigned = rr.TimeSigned
	t.Algorithm = rr.Algorithm
	t.OrigId = m.Id

	tbuf := make([]byte, t.len())
	if off, err := PackRR(t, tbuf, 0, nil, false); err == nil {
		tbuf = tbuf[:off] // reset to actual size used
	} else {
		return nil, "", err
	}
	mbuf = append(mbuf, tbuf...)

	binary.BigEndian.PutUint16(mbuf[10:], uint16(len(m.Extra)+1))

	return mbuf, t.MAC, nil
}

func TsigVerify(msg []byte, secret, requestMAC string, timersOnly bool) error {
	rawsecret, err := fromBase64([]byte(secret))
	if err != nil {
		return err
	}

	stripped, tsig, err := stripTsig(msg)
	if err != nil {
		return err
	}

	msgMAC, err := hex.DecodeString(tsig.MAC)
	if err != nil {
		return err
	}

	buf := tsigBuffer(stripped, tsig, requestMAC, timersOnly)

	now := uint64(time.Now().Unix())
	ti := now - tsig.TimeSigned
	if now < tsig.TimeSigned {
		ti = tsig.TimeSigned - now
	}
	if uint64(tsig.Fudge) < ti {
		return ErrTime
	}

	var h hash.Hash
	switch strings.ToLower(tsig.Algorithm) {
	case HmacMD5:
		h = hmac.New(md5.New, rawsecret)
	case HmacSHA1:
		h = hmac.New(sha1.New, rawsecret)
	case HmacSHA256:
		h = hmac.New(sha256.New, rawsecret)
	case HmacSHA512:
		h = hmac.New(sha512.New, rawsecret)
	default:
		return ErrKeyAlg
	}
	h.Write(buf)
	if !hmac.Equal(h.Sum(nil), msgMAC) {
		return ErrSig
	}
	return nil
}

func tsigBuffer(msgbuf []byte, rr *TSIG, requestMAC string, timersOnly bool) []byte {
	var buf []byte
	if rr.TimeSigned == 0 {
		rr.TimeSigned = uint64(time.Now().Unix())
	}
	if rr.Fudge == 0 {
		rr.Fudge = 300 // Standard (RFC) default.
	}

	binary.BigEndian.PutUint16(msgbuf[0:2], rr.OrigId)

	if requestMAC != "" {
		m := new(macWireFmt)
		m.MACSize = uint16(len(requestMAC) / 2)
		m.MAC = requestMAC
		buf = make([]byte, len(requestMAC)) // long enough
		n, _ := packMacWire(m, buf)
		buf = buf[:n]
	}

	tsigvar := make([]byte, DefaultMsgSize)
	if timersOnly {
		tsig := new(timerWireFmt)
		tsig.TimeSigned = rr.TimeSigned
		tsig.Fudge = rr.Fudge
		n, _ := packTimerWire(tsig, tsigvar)
		tsigvar = tsigvar[:n]
	} else {
		tsig := new(tsigWireFmt)
		tsig.Name = strings.ToLower(rr.Hdr.Name)
		tsig.Class = ClassANY
		tsig.Ttl = rr.Hdr.Ttl
		tsig.Algorithm = strings.ToLower(rr.Algorithm)
		tsig.TimeSigned = rr.TimeSigned
		tsig.Fudge = rr.Fudge
		tsig.Error = rr.Error
		tsig.OtherLen = rr.OtherLen
		tsig.OtherData = rr.OtherData
		n, _ := packTsigWire(tsig, tsigvar)
		tsigvar = tsigvar[:n]
	}

	if requestMAC != "" {
		x := append(buf, msgbuf...)
		buf = append(x, tsigvar...)
	} else {
		buf = append(msgbuf, tsigvar...)
	}
	return buf
}

func stripTsig(msg []byte) ([]byte, *TSIG, error) {

	var (
		dh  Header
		err error
	)
	off, tsigoff := 0, 0

	if dh, off, err = unpackMsgHdr(msg, off); err != nil {
		return nil, nil, err
	}
	if dh.Arcount == 0 {
		return nil, nil, ErrNoSig
	}

	if int(dh.Bits&0xF) == RcodeNotAuth {
		return nil, nil, ErrAuth
	}

	for i := 0; i < int(dh.Qdcount); i++ {
		_, off, err = unpackQuestion(msg, off)
		if err != nil {
			return nil, nil, err
		}
	}

	_, off, err = unpackRRslice(int(dh.Ancount), msg, off)
	if err != nil {
		return nil, nil, err
	}
	_, off, err = unpackRRslice(int(dh.Nscount), msg, off)
	if err != nil {
		return nil, nil, err
	}

	rr := new(TSIG)
	var extra RR
	for i := 0; i < int(dh.Arcount); i++ {
		tsigoff = off
		extra, off, err = UnpackRR(msg, off)
		if err != nil {
			return nil, nil, err
		}
		if extra.Header().Rrtype == TypeTSIG {
			rr = extra.(*TSIG)

			arcount := binary.BigEndian.Uint16(msg[10:])
			binary.BigEndian.PutUint16(msg[10:], arcount-1)
			break
		}
	}
	if rr == nil {
		return nil, nil, ErrNoSig
	}
	return msg[:tsigoff], rr, nil
}

func tsigTimeToString(t uint64) string {
	ti := time.Unix(int64(t), 0).UTC()
	return ti.Format("20060102150405")
}

func packTsigWire(tw *tsigWireFmt, msg []byte) (int, error) {

	off, err := PackDomainName(tw.Name, msg, 0, nil, false)
	if err != nil {
		return off, err
	}
	off, err = packUint16(tw.Class, msg, off)
	if err != nil {
		return off, err
	}
	off, err = packUint32(tw.Ttl, msg, off)
	if err != nil {
		return off, err
	}

	off, err = PackDomainName(tw.Algorithm, msg, off, nil, false)
	if err != nil {
		return off, err
	}
	off, err = packUint48(tw.TimeSigned, msg, off)
	if err != nil {
		return off, err
	}
	off, err = packUint16(tw.Fudge, msg, off)
	if err != nil {
		return off, err
	}

	off, err = packUint16(tw.Error, msg, off)
	if err != nil {
		return off, err
	}
	off, err = packUint16(tw.OtherLen, msg, off)
	if err != nil {
		return off, err
	}
	off, err = packStringHex(tw.OtherData, msg, off)
	if err != nil {
		return off, err
	}
	return off, nil
}

func packMacWire(mw *macWireFmt, msg []byte) (int, error) {
	off, err := packUint16(mw.MACSize, msg, 0)
	if err != nil {
		return off, err
	}
	off, err = packStringHex(mw.MAC, msg, off)
	if err != nil {
		return off, err
	}
	return off, nil
}

func packTimerWire(tw *timerWireFmt, msg []byte) (int, error) {
	off, err := packUint48(tw.TimeSigned, msg, 0)
	if err != nil {
		return off, err
	}
	off, err = packUint16(tw.Fudge, msg, off)
	if err != nil {
		return off, err
	}
	return off, nil
}
