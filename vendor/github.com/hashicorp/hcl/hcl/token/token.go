// Package token defines constants representing the lexical tokens for HCL
package token

import (
	"fmt"
	"strconv"
	"strings"

	hclstrconv "github.com/hashicorp/hcl/hcl/strconv"
)

type Token struct {
	Type Type
	Pos  Pos
	Text string
	JSON bool
}

type Type int

const (
	ILLEGAL Type = iota
	EOF
	COMMENT

	identifier_beg
	IDENT // literals
	literal_beg
	NUMBER  // 12345
	FLOAT   // 123.45
	BOOL    // true,false
	STRING  // "abc"
	HEREDOC // <<FOO\nbar\nFOO
	literal_end
	identifier_end

	operator_beg
	LBRACK // [
	LBRACE // {
	COMMA  // ,
	PERIOD // .

	RBRACK // ]
	RBRACE // }

	ASSIGN // =
	ADD    // +
	SUB    // -
	operator_end
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",

	EOF:     "EOF",
	COMMENT: "COMMENT",

	IDENT:  "IDENT",
	NUMBER: "NUMBER",
	FLOAT:  "FLOAT",
	BOOL:   "BOOL",
	STRING: "STRING",

	LBRACK:  "LBRACK",
	LBRACE:  "LBRACE",
	COMMA:   "COMMA",
	PERIOD:  "PERIOD",
	HEREDOC: "HEREDOC",

	RBRACK: "RBRACK",
	RBRACE: "RBRACE",

	ASSIGN: "ASSIGN",
	ADD:    "ADD",
	SUB:    "SUB",
}

func (t Type) String() string {
	s := ""
	if 0 <= t && t < Type(len(tokens)) {
		s = tokens[t]
	}
	if s == "" {
		s = "token(" + strconv.Itoa(int(t)) + ")"
	}
	return s
}

func (t Type) IsIdentifier() bool { return identifier_beg < t && t < identifier_end }

func (t Type) IsLiteral() bool { return literal_beg < t && t < literal_end }

func (t Type) IsOperator() bool { return operator_beg < t && t < operator_end }

func (t Token) String() string {
	return fmt.Sprintf("%s %s %s", t.Pos.String(), t.Type.String(), t.Text)
}

//
func (t Token) Value() interface{} {
	switch t.Type {
	case BOOL:
		if t.Text == "true" {
			return true
		} else if t.Text == "false" {
			return false
		}

		panic("unknown bool value: " + t.Text)
	case FLOAT:
		v, err := strconv.ParseFloat(t.Text, 64)
		if err != nil {
			panic(err)
		}

		return float64(v)
	case NUMBER:
		v, err := strconv.ParseInt(t.Text, 0, 64)
		if err != nil {
			panic(err)
		}

		return int64(v)
	case IDENT:
		return t.Text
	case HEREDOC:
		return unindentHeredoc(t.Text)
	case STRING:

		f := hclstrconv.Unquote
		if t.JSON {
			f = strconv.Unquote
		}

		if t.Text == "" {
			return ""
		}

		v, err := f(t.Text)
		if err != nil {
			panic(fmt.Sprintf("unquote %s err: %s", t.Text, err))
		}

		return v
	default:
		panic(fmt.Sprintf("unimplemented Value for type: %s", t.Type))
	}
}

func unindentHeredoc(heredoc string) string {

	idx := strings.IndexByte(heredoc, '\n')
	if idx == -1 {
		panic("heredoc doesn't contain newline")
	}

	unindent := heredoc[2] == '-'

	if !unindent {
		return string(heredoc[idx+1 : len(heredoc)-idx+1])
	}

	lines := strings.Split(string(heredoc[idx+1:len(heredoc)-idx+2]), "\n")
	whitespacePrefix := lines[len(lines)-1]

	isIndented := true
	for _, v := range lines {
		if strings.HasPrefix(v, whitespacePrefix) {
			continue
		}

		isIndented = false
		break
	}

	if !isIndented {
		return strings.TrimRight(string(heredoc[idx+1:len(heredoc)-idx+1]), " \t")
	}

	unindentedLines := make([]string, len(lines))
	for k, v := range lines {
		if k == len(lines)-1 {
			unindentedLines[k] = ""
			break
		}

		unindentedLines[k] = strings.TrimPrefix(v, whitespacePrefix)
	}

	return strings.Join(unindentedLines, "\n")
}
