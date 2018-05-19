package dns

func Dedup(rrs []RR, m map[string]RR) []RR {
	if m == nil {
		m = make(map[string]RR)
	}

	keys := make([]*string, 0, len(rrs))

	for _, r := range rrs {
		key := normalizedString(r)
		keys = append(keys, &key)
		if _, ok := m[key]; ok {

			if m[key].Header().Ttl > r.Header().Ttl {
				m[key].Header().Ttl = r.Header().Ttl
			}
			continue
		}

		m[key] = r
	}

	if len(m) == len(rrs) {
		return rrs
	}

	j := 0
	for i, r := range rrs {

		if _, ok := m[*keys[i]]; ok {
			delete(m, *keys[i])
			rrs[j] = r
			j++
		}

		if len(m) == 0 {
			break
		}
	}

	return rrs[:j]
}

func normalizedString(r RR) string {

	b := []byte(r.String())

	esc := false
	ttlStart, ttlEnd := 0, 0
	for i := 0; i < len(b) && ttlEnd == 0; i++ {
		switch {
		case b[i] == '\\':
			esc = !esc
		case b[i] == '\t' && !esc:
			if ttlStart == 0 {
				ttlStart = i
				continue
			}
			if ttlEnd == 0 {
				ttlEnd = i
			}
		case b[i] >= 'A' && b[i] <= 'Z' && !esc:
			b[i] += 32
		default:
			esc = false
		}
	}

	copy(b[ttlStart:], b[ttlEnd:])
	cut := ttlEnd - ttlStart
	return string(b[:len(b)-cut])
}
