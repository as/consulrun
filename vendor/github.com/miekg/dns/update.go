package dns

func (u *Msg) NameUsed(rr []RR) {
	if u.Answer == nil {
		u.Answer = make([]RR, 0, len(rr))
	}
	for _, r := range rr {
		u.Answer = append(u.Answer, &ANY{Hdr: RR_Header{Name: r.Header().Name, Ttl: 0, Rrtype: TypeANY, Class: ClassANY}})
	}
}

func (u *Msg) NameNotUsed(rr []RR) {
	if u.Answer == nil {
		u.Answer = make([]RR, 0, len(rr))
	}
	for _, r := range rr {
		u.Answer = append(u.Answer, &ANY{Hdr: RR_Header{Name: r.Header().Name, Ttl: 0, Rrtype: TypeANY, Class: ClassNONE}})
	}
}

func (u *Msg) Used(rr []RR) {
	if len(u.Question) == 0 {
		panic("dns: empty question section")
	}
	if u.Answer == nil {
		u.Answer = make([]RR, 0, len(rr))
	}
	for _, r := range rr {
		r.Header().Class = u.Question[0].Qclass
		u.Answer = append(u.Answer, r)
	}
}

func (u *Msg) RRsetUsed(rr []RR) {
	if u.Answer == nil {
		u.Answer = make([]RR, 0, len(rr))
	}
	for _, r := range rr {
		u.Answer = append(u.Answer, &ANY{Hdr: RR_Header{Name: r.Header().Name, Ttl: 0, Rrtype: r.Header().Rrtype, Class: ClassANY}})
	}
}

func (u *Msg) RRsetNotUsed(rr []RR) {
	if u.Answer == nil {
		u.Answer = make([]RR, 0, len(rr))
	}
	for _, r := range rr {
		u.Answer = append(u.Answer, &ANY{Hdr: RR_Header{Name: r.Header().Name, Ttl: 0, Rrtype: r.Header().Rrtype, Class: ClassNONE}})
	}
}

func (u *Msg) Insert(rr []RR) {
	if len(u.Question) == 0 {
		panic("dns: empty question section")
	}
	if u.Ns == nil {
		u.Ns = make([]RR, 0, len(rr))
	}
	for _, r := range rr {
		r.Header().Class = u.Question[0].Qclass
		u.Ns = append(u.Ns, r)
	}
}

func (u *Msg) RemoveRRset(rr []RR) {
	if u.Ns == nil {
		u.Ns = make([]RR, 0, len(rr))
	}
	for _, r := range rr {
		u.Ns = append(u.Ns, &ANY{Hdr: RR_Header{Name: r.Header().Name, Ttl: 0, Rrtype: r.Header().Rrtype, Class: ClassANY}})
	}
}

func (u *Msg) RemoveName(rr []RR) {
	if u.Ns == nil {
		u.Ns = make([]RR, 0, len(rr))
	}
	for _, r := range rr {
		u.Ns = append(u.Ns, &ANY{Hdr: RR_Header{Name: r.Header().Name, Ttl: 0, Rrtype: TypeANY, Class: ClassANY}})
	}
}

func (u *Msg) Remove(rr []RR) {
	if u.Ns == nil {
		u.Ns = make([]RR, 0, len(rr))
	}
	for _, r := range rr {
		r.Header().Class = ClassNONE
		r.Header().Ttl = 0
		u.Ns = append(u.Ns, r)
	}
}
