// Package scanner implements a scanner for HCL (HashiCorp Configuration
package scanner

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"unicode"
	"unicode/utf8"

	"github.com/hashicorp/hcl/hcl/token"
)

const eof = rune(0)

type Scanner struct {
	buf *bytes.Buffer // Source buffer for advancing and scanning
	src []byte        // Source buffer for immutable access

	srcPos  token.Pos // current position
	prevPos token.Pos // previous position, used for peek() method

	lastCharLen int // length of last character in bytes
	lastLineLen int // length of last line in characters (for correct column reporting)

	tokStart int // token text start position
	tokEnd   int // token text end  position

	Error func(pos token.Pos, msg string)

	ErrorCount int

	tokPos token.Pos
}

func New(src []byte) *Scanner {

	b := bytes.NewBuffer(src)
	s := &Scanner{
		buf: b,
		src: src,
	}

	s.srcPos.Line = 1
	return s
}

func (s *Scanner) next() rune {
	ch, size, err := s.buf.ReadRune()
	if err != nil {

		s.srcPos.Column++
		s.srcPos.Offset += size
		s.lastCharLen = size
		return eof
	}

	if ch == utf8.RuneError && size == 1 {
		s.srcPos.Column++
		s.srcPos.Offset += size
		s.lastCharLen = size
		s.err("illegal UTF-8 encoding")
		return ch
	}

	s.prevPos = s.srcPos

	s.srcPos.Column++
	s.lastCharLen = size
	s.srcPos.Offset += size

	if ch == '\n' {
		s.srcPos.Line++
		s.lastLineLen = s.srcPos.Column
		s.srcPos.Column = 0
	}

	return ch
}

func (s *Scanner) unread() {
	if err := s.buf.UnreadRune(); err != nil {
		panic(err) // this is user fault, we should catch it
	}
	s.srcPos = s.prevPos // put back last position
}

func (s *Scanner) peek() rune {
	peek, _, err := s.buf.ReadRune()
	if err != nil {
		return eof
	}

	s.buf.UnreadRune()
	return peek
}

func (s *Scanner) Scan() token.Token {
	ch := s.next()

	for isWhitespace(ch) {
		ch = s.next()
	}

	var tok token.Type

	s.tokStart = s.srcPos.Offset - s.lastCharLen

	s.tokPos.Offset = s.srcPos.Offset - s.lastCharLen
	if s.srcPos.Column > 0 {

		s.tokPos.Line = s.srcPos.Line
		s.tokPos.Column = s.srcPos.Column
	} else {

		s.tokPos.Line = s.srcPos.Line - 1
		s.tokPos.Column = s.lastLineLen
	}

	switch {
	case isLetter(ch):
		tok = token.IDENT
		lit := s.scanIdentifier()
		if lit == "true" || lit == "false" {
			tok = token.BOOL
		}
	case isDecimal(ch):
		tok = s.scanNumber(ch)
	default:
		switch ch {
		case eof:
			tok = token.EOF
		case '"':
			tok = token.STRING
			s.scanString()
		case '#', '/':
			tok = token.COMMENT
			s.scanComment(ch)
		case '.':
			tok = token.PERIOD
			ch = s.peek()
			if isDecimal(ch) {
				tok = token.FLOAT
				ch = s.scanMantissa(ch)
				ch = s.scanExponent(ch)
			}
		case '<':
			tok = token.HEREDOC
			s.scanHeredoc()
		case '[':
			tok = token.LBRACK
		case ']':
			tok = token.RBRACK
		case '{':
			tok = token.LBRACE
		case '}':
			tok = token.RBRACE
		case ',':
			tok = token.COMMA
		case '=':
			tok = token.ASSIGN
		case '+':
			tok = token.ADD
		case '-':
			if isDecimal(s.peek()) {
				ch := s.next()
				tok = s.scanNumber(ch)
			} else {
				tok = token.SUB
			}
		default:
			s.err("illegal char")
		}
	}

	s.tokEnd = s.srcPos.Offset

	var tokenText string
	if s.tokStart >= 0 {
		tokenText = string(s.src[s.tokStart:s.tokEnd])
	}
	s.tokStart = s.tokEnd // ensure idempotency of tokenText() call

	return token.Token{
		Type: tok,
		Pos:  s.tokPos,
		Text: tokenText,
	}
}

func (s *Scanner) scanComment(ch rune) {

	if ch == '#' || (ch == '/' && s.peek() != '*') {
		ch = s.next()
		for ch != '\n' && ch >= 0 && ch != eof {
			ch = s.next()
		}
		if ch != eof && ch >= 0 {
			s.unread()
		}
		return
	}

	if ch == '/' {
		s.next()
		ch = s.next() // read character after "/*"
	}

	for {
		if ch < 0 || ch == eof {
			s.err("comment not terminated")
			break
		}

		ch0 := ch
		ch = s.next()
		if ch0 == '*' && ch == '/' {
			break
		}
	}
}

func (s *Scanner) scanNumber(ch rune) token.Type {
	if ch == '0' {

		ch = s.next()
		if ch == 'x' || ch == 'X' {

			ch = s.next()
			found := false
			for isHexadecimal(ch) {
				ch = s.next()
				found = true
			}

			if !found {
				s.err("illegal hexadecimal number")
			}

			if ch != eof {
				s.unread()
			}

			return token.NUMBER
		}

		illegalOctal := false
		for isDecimal(ch) {
			ch = s.next()
			if ch == '8' || ch == '9' {

				illegalOctal = true
			}
		}

		if ch == 'e' || ch == 'E' {
			ch = s.scanExponent(ch)
			return token.FLOAT
		}

		if ch == '.' {
			ch = s.scanFraction(ch)

			if ch == 'e' || ch == 'E' {
				ch = s.next()
				ch = s.scanExponent(ch)
			}
			return token.FLOAT
		}

		if illegalOctal {
			s.err("illegal octal number")
		}

		if ch != eof {
			s.unread()
		}
		return token.NUMBER
	}

	s.scanMantissa(ch)
	ch = s.next() // seek forward
	if ch == 'e' || ch == 'E' {
		ch = s.scanExponent(ch)
		return token.FLOAT
	}

	if ch == '.' {
		ch = s.scanFraction(ch)
		if ch == 'e' || ch == 'E' {
			ch = s.next()
			ch = s.scanExponent(ch)
		}
		return token.FLOAT
	}

	if ch != eof {
		s.unread()
	}
	return token.NUMBER
}

func (s *Scanner) scanMantissa(ch rune) rune {
	scanned := false
	for isDecimal(ch) {
		ch = s.next()
		scanned = true
	}

	if scanned && ch != eof {
		s.unread()
	}
	return ch
}

func (s *Scanner) scanFraction(ch rune) rune {
	if ch == '.' {
		ch = s.peek() // we peek just to see if we can move forward
		ch = s.scanMantissa(ch)
	}
	return ch
}

func (s *Scanner) scanExponent(ch rune) rune {
	if ch == 'e' || ch == 'E' {
		ch = s.next()
		if ch == '-' || ch == '+' {
			ch = s.next()
		}
		ch = s.scanMantissa(ch)
	}
	return ch
}

func (s *Scanner) scanHeredoc() {

	if s.next() != '<' {
		s.err("heredoc expected second '<', didn't see it")
		return
	}

	offs := s.srcPos.Offset

	ch := s.next()

	if ch == '-' {
		ch = s.next()
	}

	for isLetter(ch) || isDigit(ch) {
		ch = s.next()
	}

	if ch == eof {
		s.err("heredoc not terminated")
		return
	}

	if ch == '\r' {
		if s.peek() == '\n' {
			ch = s.next()
		}
	}

	if ch != '\n' {
		s.err("invalid characters in heredoc anchor")
		return
	}

	identBytes := s.src[offs : s.srcPos.Offset-s.lastCharLen]
	if len(identBytes) == 0 {
		s.err("zero-length heredoc anchor")
		return
	}

	var identRegexp *regexp.Regexp
	if identBytes[0] == '-' {
		identRegexp = regexp.MustCompile(fmt.Sprintf(`[[:space:]]*%s\z`, identBytes[1:]))
	} else {
		identRegexp = regexp.MustCompile(fmt.Sprintf(`[[:space:]]*%s\z`, identBytes))
	}

	lineStart := s.srcPos.Offset
	for {
		ch := s.next()

		if ch == '\n' {

			lineBytesLen := s.srcPos.Offset - s.lastCharLen - lineStart
			if lineBytesLen >= len(identBytes) && identRegexp.Match(s.src[lineStart:s.srcPos.Offset-s.lastCharLen]) {
				break
			}

			lineStart = s.srcPos.Offset
		}

		if ch == eof {
			s.err("heredoc not terminated")
			return
		}
	}

	return
}

func (s *Scanner) scanString() {
	braces := 0
	for {

		ch := s.next()

		if ch < 0 || ch == eof {
			s.err("literal not terminated")
			return
		}

		if ch == '"' && braces == 0 {
			break
		}

		if braces == 0 && ch == '$' && s.peek() == '{' {
			braces++
			s.next()
		} else if braces > 0 && ch == '{' {
			braces++
		}
		if braces > 0 && ch == '}' {
			braces--
		}

		if ch == '\\' {
			s.scanEscape()
		}
	}

	return
}

func (s *Scanner) scanEscape() rune {

	ch := s.next() // read character after '/'
	switch ch {
	case 'a', 'b', 'f', 'n', 'r', 't', 'v', '\\', '"':

	case '0', '1', '2', '3', '4', '5', '6', '7':

		ch = s.scanDigits(ch, 8, 3)
	case 'x':

		ch = s.scanDigits(s.next(), 16, 2)
	case 'u':

		ch = s.scanDigits(s.next(), 16, 4)
	case 'U':

		ch = s.scanDigits(s.next(), 16, 8)
	default:
		s.err("illegal char escape")
	}
	return ch
}

func (s *Scanner) scanDigits(ch rune, base, n int) rune {
	start := n
	for n > 0 && digitVal(ch) < base {
		ch = s.next()
		if ch == eof {

			break
		}

		n--
	}
	if n > 0 {
		s.err("illegal char escape")
	}

	if n != start {

		s.unread()
	}

	return ch
}

func (s *Scanner) scanIdentifier() string {
	offs := s.srcPos.Offset - s.lastCharLen
	ch := s.next()
	for isLetter(ch) || isDigit(ch) || ch == '-' || ch == '.' {
		ch = s.next()
	}

	if ch != eof {
		s.unread() // we got identifier, put back latest char
	}

	return string(s.src[offs:s.srcPos.Offset])
}

func (s *Scanner) recentPosition() (pos token.Pos) {
	pos.Offset = s.srcPos.Offset - s.lastCharLen
	switch {
	case s.srcPos.Column > 0:

		pos.Line = s.srcPos.Line
		pos.Column = s.srcPos.Column
	case s.lastLineLen > 0:

		pos.Line = s.srcPos.Line - 1
		pos.Column = s.lastLineLen
	default:

		pos.Line = 1
		pos.Column = 1
	}
	return
}

func (s *Scanner) err(msg string) {
	s.ErrorCount++
	pos := s.recentPosition()

	if s.Error != nil {
		s.Error(pos, msg)
		return
	}

	fmt.Fprintf(os.Stderr, "%s: %s\n", pos, msg)
}

func isLetter(ch rune) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_' || ch >= 0x80 && unicode.IsLetter(ch)
}

func isDigit(ch rune) bool {
	return '0' <= ch && ch <= '9' || ch >= 0x80 && unicode.IsDigit(ch)
}

func isDecimal(ch rune) bool {
	return '0' <= ch && ch <= '9'
}

func isHexadecimal(ch rune) bool {
	return '0' <= ch && ch <= '9' || 'a' <= ch && ch <= 'f' || 'A' <= ch && ch <= 'F'
}

func isWhitespace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func digitVal(ch rune) int {
	switch {
	case '0' <= ch && ch <= '9':
		return int(ch - '0')
	case 'a' <= ch && ch <= 'f':
		return int(ch - 'a' + 10)
	case 'A' <= ch && ch <= 'F':
		return int(ch - 'A' + 10)
	}
	return 16 // larger than any legal digit val
}
