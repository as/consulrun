package dns

func SplitDomainName(s string) (labels []string) {
	if len(s) == 0 {
		return nil
	}
	fqdnEnd := 0 // offset of the final '.' or the length of the name
	idx := Split(s)
	begin := 0
	if s[len(s)-1] == '.' {
		fqdnEnd = len(s) - 1
	} else {
		fqdnEnd = len(s)
	}

	switch len(idx) {
	case 0:
		return nil
	case 1:

	default:
		end := 0
		for i := 1; i < len(idx); i++ {
			end = idx[i]
			labels = append(labels, s[begin:end-1])
			begin = end
		}
	}

	labels = append(labels, s[begin:fqdnEnd])
	return labels
}

//
//
func CompareDomainName(s1, s2 string) (n int) {

	if s1 == "." || s2 == "." {
		return 0
	}

	l1 := Split(s1)
	l2 := Split(s2)

	j1 := len(l1) - 1 // end
	i1 := len(l1) - 2 // start
	j2 := len(l2) - 1
	i2 := len(l2) - 2

	if equal(s1[l1[j1]:], s2[l2[j2]:]) {
		n++
	} else {
		return
	}
	for {
		if i1 < 0 || i2 < 0 {
			break
		}
		if equal(s1[l1[i1]:l1[j1]], s2[l2[i2]:l2[j2]]) {
			n++
		} else {
			break
		}
		j1--
		i1--
		j2--
		i2--
	}
	return
}

func CountLabel(s string) (labels int) {
	if s == "." {
		return
	}
	off := 0
	end := false
	for {
		off, end = NextLabel(s, off)
		labels++
		if end {
			return
		}
	}
}

func Split(s string) []int {
	if s == "." {
		return nil
	}
	idx := make([]int, 1, 3)
	off := 0
	end := false

	for {
		off, end = NextLabel(s, off)
		if end {
			return idx
		}
		idx = append(idx, off)
	}
}

func NextLabel(s string, offset int) (i int, end bool) {
	quote := false
	for i = offset; i < len(s)-1; i++ {
		switch s[i] {
		case '\\':
			quote = !quote
		default:
			quote = false
		case '.':
			if quote {
				quote = !quote
				continue
			}
			return i + 1, false
		}
	}
	return i + 1, true
}

func PrevLabel(s string, n int) (i int, start bool) {
	if n == 0 {
		return len(s), false
	}
	lab := Split(s)
	if lab == nil {
		return 0, true
	}
	if n > len(lab) {
		return 0, true
	}
	return lab[len(lab)-n], false
}

func equal(a, b string) bool {

	la := len(a)
	lb := len(b)
	if la != lb {
		return false
	}

	for i := la - 1; i >= 0; i-- {
		ai := a[i]
		bi := b[i]
		if ai >= 'A' && ai <= 'Z' {
			ai |= ('a' - 'A')
		}
		if bi >= 'A' && bi <= 'Z' {
			bi |= ('a' - 'A')
		}
		if ai != bi {
			return false
		}
	}
	return true
}
