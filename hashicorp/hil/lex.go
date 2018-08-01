package hil

import (
	"bytes"
	"fmt"
	"strconv"
	"unicode"
	"unicode/utf8"

	"github.com/as/consulrun/hashicorp/hil/ast"
)

const lexEOF = 0

type parserLex struct {
	Err   error
	Input string

	mode               parserMode
	interpolationDepth int
	pos                int
	width              int
	col, line          int
	lastLine           int
	astPos             *ast.Pos
}

type parserToken struct {
	Value interface{}
	Pos   ast.Pos
}

type parserMode uint8

const (
	parserModeInvalid parserMode = 0
	parserModeLiteral            = 1 << iota
	parserModeInterpolation
)

func (x *parserLex) Lex(yylval *parserSymType) int {

	if x.mode == parserModeInvalid {
		x.mode = parserModeLiteral
	}

	defer func() {
		if yylval.token != nil && yylval.token.Pos.Column == 0 {
			yylval.token.Pos = *x.astPos
		}
	}()

	x.astPos = nil
	return x.lex(yylval)
}

func (x *parserLex) lex(yylval *parserSymType) int {
	switch x.mode {
	case parserModeLiteral:
		return x.lexModeLiteral(yylval)
	case parserModeInterpolation:
		return x.lexModeInterpolation(yylval)
	default:
		x.Error(fmt.Sprintf("Unknown parse mode: %d", x.mode))
		return lexEOF
	}
}

func (x *parserLex) lexModeLiteral(yylval *parserSymType) int {
	for {
		c := x.next()
		if c == lexEOF {
			return lexEOF
		}

		if c == '$' && x.peek() == '{' {
			x.next()
			x.interpolationDepth++
			x.mode = parserModeInterpolation
			return PROGRAM_BRACKET_LEFT
		}

		x.backup()
		result, terminated := x.lexString(yylval, x.interpolationDepth > 0)

		if terminated && x.interpolationDepth > 0 {
			x.mode = parserModeInterpolation

			if yylval.token.Value.(string) == "" {
				return x.lex(yylval)
			}
		}

		return result
	}
}

func (x *parserLex) lexModeInterpolation(yylval *parserSymType) int {
	for {
		c := x.next()
		if c == lexEOF {
			return lexEOF
		}

		if unicode.IsSpace(c) {
			continue
		}

		if c == '"' {
			result, terminated := x.lexString(yylval, true)
			if !terminated {

				x.mode = parserModeLiteral

				if yylval.token.Value.(string) == "" {
					return x.lex(yylval)
				}
			}

			return result
		}

		if c >= '0' && c <= '9' {
			x.backup()
			return x.lexNumber(yylval)
		}

		switch c {
		case '}':

			x.interpolationDepth--
			x.mode = parserModeLiteral
			return PROGRAM_BRACKET_RIGHT
		case '(':
			return PAREN_LEFT
		case ')':
			return PAREN_RIGHT
		case '[':
			return SQUARE_BRACKET_LEFT
		case ']':
			return SQUARE_BRACKET_RIGHT
		case ',':
			return COMMA
		case '+':
			yylval.token = &parserToken{Value: ast.ArithmeticOpAdd}
			return ARITH_OP
		case '-':
			yylval.token = &parserToken{Value: ast.ArithmeticOpSub}
			return ARITH_OP
		case '*':
			yylval.token = &parserToken{Value: ast.ArithmeticOpMul}
			return ARITH_OP
		case '/':
			yylval.token = &parserToken{Value: ast.ArithmeticOpDiv}
			return ARITH_OP
		case '%':
			yylval.token = &parserToken{Value: ast.ArithmeticOpMod}
			return ARITH_OP
		default:
			x.backup()
			return x.lexId(yylval)
		}
	}
}

func (x *parserLex) lexId(yylval *parserSymType) int {
	var b bytes.Buffer
	var last rune
	for {
		c := x.next()
		if c == lexEOF {
			break
		}

		if c == '*' && last != '.' {
			x.backup()
			break
		}

		if c != '_' &&
			c != '-' &&
			c != '.' &&
			c != '*' &&
			!unicode.IsLetter(c) &&
			!unicode.IsNumber(c) {
			x.backup()
			break
		}

		if _, err := b.WriteRune(c); err != nil {
			x.Error(err.Error())
			return lexEOF
		}

		last = c
	}

	yylval.token = &parserToken{Value: b.String()}
	return IDENTIFIER
}

func (x *parserLex) lexNumber(yylval *parserSymType) int {
	var b bytes.Buffer
	gotPeriod := false
	for {
		c := x.next()
		if c == lexEOF {
			break
		}

		if c == '.' {

			if gotPeriod {
				x.backup()
				break
			}

			gotPeriod = true
		} else if c < '0' || c > '9' {

			x.backup()
			break
		}

		if _, err := b.WriteRune(c); err != nil {
			x.Error(fmt.Sprintf("internal error: %s", err))
			return lexEOF
		}
	}

	if !gotPeriod {
		v, err := strconv.ParseInt(b.String(), 0, 0)
		if err != nil {
			x.Error(fmt.Sprintf("expected number: %s", err))
			return lexEOF
		}

		yylval.token = &parserToken{Value: int(v)}
		return INTEGER
	}

	f, err := strconv.ParseFloat(b.String(), 64)
	if err != nil {
		x.Error(fmt.Sprintf("expected float: %s", err))
		return lexEOF
	}

	yylval.token = &parserToken{Value: f}
	return FLOAT
}

func (x *parserLex) lexString(yylval *parserSymType, quoted bool) (int, bool) {
	var b bytes.Buffer
	terminated := false
	for {
		c := x.next()
		if c == lexEOF {
			if quoted {
				x.Error("unterminated string")
			}

			break
		}

		if quoted {

			if c == '"' {
				terminated = true
				break
			}

			if c == '\\' {
				switch n := x.next(); n {
				case '\\', '"':
					c = n
				case 'n':
					c = '\n'
				default:
					x.backup()
				}
			}
		}

		if c == '$' {
			n := x.peek()

			if n == '{' {
				x.backup()
				break
			}

			if n == '$' {
				x.next()
			}
		}

		if _, err := b.WriteRune(c); err != nil {
			x.Error(err.Error())
			return lexEOF, false
		}
	}

	yylval.token = &parserToken{Value: b.String()}
	return STRING, terminated
}

func (x *parserLex) next() rune {
	if int(x.pos) >= len(x.Input) {
		x.width = 0
		return lexEOF
	}

	r, w := utf8.DecodeRuneInString(x.Input[x.pos:])
	x.width = w
	x.pos += x.width

	if x.line == 0 {
		x.line = 1
		x.col = 1
	} else {
		x.col += 1
	}

	if r == '\n' {
		x.lastLine = x.col
		x.line += 1
		x.col = 1
	}

	if x.astPos == nil {
		x.astPos = &ast.Pos{Column: x.col, Line: x.line}
	}

	return r
}

func (x *parserLex) peek() rune {
	r := x.next()
	x.backup()
	return r
}

func (x *parserLex) backup() {
	x.pos -= x.width
	x.col -= 1

	if x.col == 0 {
		x.col = x.lastLine
		x.line -= 1
	}
}

func (x *parserLex) Error(s string) {
	x.Err = fmt.Errorf("parse error: %s", s)
}
