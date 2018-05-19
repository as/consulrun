package dns

import (
	"bufio"
	"context"
	"io"
	"text/scanner"
)

type scan struct {
	src      *bufio.Reader
	position scanner.Position
	eof      bool // Have we just seen a eof
	ctx      context.Context
}

func scanInit(r io.Reader) (*scan, context.CancelFunc) {
	s := new(scan)
	s.src = bufio.NewReader(r)
	s.position.Line = 1

	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx

	return s, cancel
}

func (s *scan) tokenText() (byte, error) {
	c, err := s.src.ReadByte()
	if err != nil {
		return c, err
	}
	select {
	case <-s.ctx.Done():
		return c, context.Canceled
	default:
		break
	}

	if s.eof == true {
		s.position.Line++
		s.position.Column = 0
		s.eof = false
	}
	if c == '\n' {
		s.eof = true
		return c, nil
	}
	s.position.Column++
	return c, nil
}
