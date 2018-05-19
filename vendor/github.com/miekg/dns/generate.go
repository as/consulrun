package dns

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func generate(l lex, c chan lex, t chan *Token, o string) string {
	step := 1
	if i := strings.IndexAny(l.token, "/"); i != -1 {
		if i+1 == len(l.token) {
			return "bad step in $GENERATE range"
		}
		if s, err := strconv.Atoi(l.token[i+1:]); err == nil {
			if s < 0 {
				return "bad step in $GENERATE range"
			}
			step = s
		} else {
			return "bad step in $GENERATE range"
		}
		l.token = l.token[:i]
	}
	sx := strings.SplitN(l.token, "-", 2)
	if len(sx) != 2 {
		return "bad start-stop in $GENERATE range"
	}
	start, err := strconv.Atoi(sx[0])
	if err != nil {
		return "bad start in $GENERATE range"
	}
	end, err := strconv.Atoi(sx[1])
	if err != nil {
		return "bad stop in $GENERATE range"
	}
	if end < 0 || start < 0 || end < start {
		return "bad range in $GENERATE range"
	}

	<-c // _BLANK

	s := ""
BuildRR:
	l = <-c
	if l.value != zNewline && l.value != zEOF {
		s += l.token
		goto BuildRR
	}
	for i := start; i <= end; i += step {
		var (
			escape bool
			dom    bytes.Buffer
			mod    string
			err    error
			offset int
		)

		for j := 0; j < len(s); j++ { // No 'range' because we need to jump around
			switch s[j] {
			case '\\':
				if escape {
					dom.WriteByte('\\')
					escape = false
					continue
				}
				escape = true
			case '$':
				mod = "%d"
				offset = 0
				if escape {
					dom.WriteByte('$')
					escape = false
					continue
				}
				escape = false
				if j+1 >= len(s) { // End of the string
					dom.WriteString(fmt.Sprintf(mod, i+offset))
					continue
				} else {
					if s[j+1] == '$' {
						dom.WriteByte('$')
						j++
						continue
					}
				}

				if s[j+1] == '{' { // Modifier block
					sep := strings.Index(s[j+2:], "}")
					if sep == -1 {
						return "bad modifier in $GENERATE"
					}
					mod, offset, err = modToPrintf(s[j+2 : j+2+sep])
					if err != nil {
						return err.Error()
					}
					j += 2 + sep // Jump to it
				}
				dom.WriteString(fmt.Sprintf(mod, i+offset))
			default:
				if escape { // Pretty useless here
					escape = false
					continue
				}
				dom.WriteByte(s[j])
			}
		}

		rx, err := NewRR("$ORIGIN " + o + "\n" + dom.String())
		if err != nil {
			return err.Error()
		}
		t <- &Token{RR: rx}

	}
	return ""
}

func modToPrintf(s string) (string, int, error) {
	xs := strings.SplitN(s, ",", 3)
	if len(xs) != 3 {
		return "", 0, errors.New("bad modifier in $GENERATE")
	}

	if xs[2] != "o" && xs[2] != "d" && xs[2] != "x" && xs[2] != "X" {
		return "", 0, errors.New("bad base in $GENERATE")
	}
	offset, err := strconv.Atoi(xs[0])
	if err != nil || offset > 255 {
		return "", 0, errors.New("bad offset in $GENERATE")
	}
	width, err := strconv.Atoi(xs[1])
	if err != nil || width > 255 {
		return "", offset, errors.New("bad width in $GENERATE")
	}
	switch {
	case width < 0:
		return "", offset, errors.New("bad width in $GENERATE")
	case width == 0:
		return "%" + xs[1] + xs[2], offset, nil
	}
	return "%0" + xs[1] + xs[2], offset, nil
}
