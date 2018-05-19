package token

import "fmt"

type Pos struct {
	Filename string // filename, if any
	Offset   int    // offset, starting at 0
	Line     int    // line number, starting at 1
	Column   int    // column number, starting at 1 (character count)
}

func (p *Pos) IsValid() bool { return p.Line > 0 }

//
func (p Pos) String() string {
	s := p.Filename
	if p.IsValid() {
		if s != "" {
			s += ":"
		}
		s += fmt.Sprintf("%d:%d", p.Line, p.Column)
	}
	if s == "" {
		s = "-"
	}
	return s
}

func (p Pos) Before(u Pos) bool {
	return u.Offset > p.Offset || u.Line > p.Line
}

func (p Pos) After(u Pos) bool {
	return u.Offset < p.Offset || u.Line < p.Line
}
