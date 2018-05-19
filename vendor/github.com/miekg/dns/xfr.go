package dns

import (
	"fmt"
	"time"
)

type Envelope struct {
	RR    []RR  // The set of RRs in the answer section of the xfr reply message.
	Error error // If something went wrong, this contains the error.
}

type Transfer struct {
	*Conn
	DialTimeout    time.Duration     // net.DialTimeout, defaults to 2 seconds
	ReadTimeout    time.Duration     // net.Conn.SetReadTimeout value for connections, defaults to 2 seconds
	WriteTimeout   time.Duration     // net.Conn.SetWriteTimeout value for connections, defaults to 2 seconds
	TsigSecret     map[string]string // Secret(s) for Tsig map[<zonename>]<base64 secret>, zonename must be in canonical form (lowercase, fqdn, see RFC 4034 Section 6.2)
	tsigTimersOnly bool
}

//
//
func (t *Transfer) In(q *Msg, a string) (env chan *Envelope, err error) {
	timeout := dnsTimeout
	if t.DialTimeout != 0 {
		timeout = t.DialTimeout
	}
	if t.Conn == nil {
		t.Conn, err = DialTimeout("tcp", a, timeout)
		if err != nil {
			return nil, err
		}
	}
	if err := t.WriteMsg(q); err != nil {
		return nil, err
	}
	env = make(chan *Envelope)
	go func() {
		if q.Question[0].Qtype == TypeAXFR {
			go t.inAxfr(q, env)
			return
		}
		if q.Question[0].Qtype == TypeIXFR {
			go t.inIxfr(q, env)
			return
		}
	}()
	return env, nil
}

func (t *Transfer) inAxfr(q *Msg, c chan *Envelope) {
	first := true
	defer t.Close()
	defer close(c)
	timeout := dnsTimeout
	if t.ReadTimeout != 0 {
		timeout = t.ReadTimeout
	}
	for {
		t.Conn.SetReadDeadline(time.Now().Add(timeout))
		in, err := t.ReadMsg()
		if err != nil {
			c <- &Envelope{nil, err}
			return
		}
		if q.Id != in.Id {
			c <- &Envelope{in.Answer, ErrId}
			return
		}
		if first {
			if in.Rcode != RcodeSuccess {
				c <- &Envelope{in.Answer, &Error{err: fmt.Sprintf(errXFR, in.Rcode)}}
				return
			}
			if !isSOAFirst(in) {
				c <- &Envelope{in.Answer, ErrSoa}
				return
			}
			first = !first

			if len(in.Answer) == 1 {
				t.tsigTimersOnly = true
				c <- &Envelope{in.Answer, nil}
				continue
			}
		}

		if !first {
			t.tsigTimersOnly = true // Subsequent envelopes use this.
			if isSOALast(in) {
				c <- &Envelope{in.Answer, nil}
				return
			}
			c <- &Envelope{in.Answer, nil}
		}
	}
}

func (t *Transfer) inIxfr(q *Msg, c chan *Envelope) {
	serial := uint32(0) // The first serial seen is the current server serial
	axfr := true
	n := 0
	qser := q.Ns[0].(*SOA).Serial
	defer t.Close()
	defer close(c)
	timeout := dnsTimeout
	if t.ReadTimeout != 0 {
		timeout = t.ReadTimeout
	}
	for {
		t.SetReadDeadline(time.Now().Add(timeout))
		in, err := t.ReadMsg()
		if err != nil {
			c <- &Envelope{nil, err}
			return
		}
		if q.Id != in.Id {
			c <- &Envelope{in.Answer, ErrId}
			return
		}
		if in.Rcode != RcodeSuccess {
			c <- &Envelope{in.Answer, &Error{err: fmt.Sprintf(errXFR, in.Rcode)}}
			return
		}
		if n == 0 {

			if !isSOAFirst(in) {
				c <- &Envelope{in.Answer, ErrSoa}
				return
			}

			serial = in.Answer[0].(*SOA).Serial

			if qser >= serial {
				c <- &Envelope{in.Answer, nil}
				return
			}
		}

		t.tsigTimersOnly = true
		for _, rr := range in.Answer {
			if v, ok := rr.(*SOA); ok {
				if v.Serial == serial {
					n++

					if axfr && n == 2 || n == 3 {
						c <- &Envelope{in.Answer, nil}
						return
					}
				} else if axfr {

					axfr = false
				}
			}
		}
		c <- &Envelope{in.Answer, nil}
	}
}

//
//
func (t *Transfer) Out(w ResponseWriter, q *Msg, ch chan *Envelope) error {
	for x := range ch {
		r := new(Msg)

		r.SetReply(q)
		r.Authoritative = true

		r.Answer = append(r.Answer, x.RR...)
		if err := w.WriteMsg(r); err != nil {
			return err
		}
	}
	w.TsigTimersOnly(true)
	return nil
}

func (t *Transfer) ReadMsg() (*Msg, error) {
	m := new(Msg)
	p := make([]byte, MaxMsgSize)
	n, err := t.Read(p)
	if err != nil && n == 0 {
		return nil, err
	}
	p = p[:n]
	if err := m.Unpack(p); err != nil {
		return nil, err
	}
	if ts := m.IsTsig(); ts != nil && t.TsigSecret != nil {
		if _, ok := t.TsigSecret[ts.Hdr.Name]; !ok {
			return m, ErrSecret
		}

		err = TsigVerify(p, t.TsigSecret[ts.Hdr.Name], t.tsigRequestMAC, t.tsigTimersOnly)
		t.tsigRequestMAC = ts.MAC
	}
	return m, err
}

func (t *Transfer) WriteMsg(m *Msg) (err error) {
	var out []byte
	if ts := m.IsTsig(); ts != nil && t.TsigSecret != nil {
		if _, ok := t.TsigSecret[ts.Hdr.Name]; !ok {
			return ErrSecret
		}
		out, t.tsigRequestMAC, err = TsigGenerate(m, t.TsigSecret[ts.Hdr.Name], t.tsigRequestMAC, t.tsigTimersOnly)
	} else {
		out, err = m.Pack()
	}
	if err != nil {
		return err
	}
	if _, err = t.Write(out); err != nil {
		return err
	}
	return nil
}

func isSOAFirst(in *Msg) bool {
	if len(in.Answer) > 0 {
		return in.Answer[0].Header().Rrtype == TypeSOA
	}
	return false
}

func isSOALast(in *Msg) bool {
	if len(in.Answer) > 0 {
		return in.Answer[len(in.Answer)-1].Header().Rrtype == TypeSOA
	}
	return false
}

const errXFR = "bad xfr rcode: %d"
