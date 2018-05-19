// Package parser implements a parser for HCL (HashiCorp Configuration
package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/hcl/hcl/ast"
	"github.com/hashicorp/hcl/hcl/scanner"
	"github.com/hashicorp/hcl/hcl/token"
)

type Parser struct {
	sc *scanner.Scanner

	tok       token.Token
	commaPrev token.Token

	comments    []*ast.CommentGroup
	leadComment *ast.CommentGroup // last lead comment
	lineComment *ast.CommentGroup // last line comment

	enableTrace bool
	indent      int
	n           int // buffer size (max = 1)
}

func newParser(src []byte) *Parser {
	return &Parser{
		sc: scanner.New(src),
	}
}

func Parse(src []byte) (*ast.File, error) {
	p := newParser(src)
	return p.Parse()
}

var errEofToken = errors.New("EOF token found")

func (p *Parser) Parse() (*ast.File, error) {
	f := &ast.File{}
	var err, scerr error
	p.sc.Error = func(pos token.Pos, msg string) {
		scerr = &PosError{Pos: pos, Err: errors.New(msg)}
	}

	f.Node, err = p.objectList()
	if scerr != nil {
		return nil, scerr
	}
	if err != nil {
		return nil, err
	}

	f.Comments = p.comments
	return f, nil
}

func (p *Parser) objectList() (*ast.ObjectList, error) {
	defer un(trace(p, "ParseObjectList"))
	node := &ast.ObjectList{}

	for {
		n, err := p.objectItem()
		if err == errEofToken {
			break // we are finished
		}

		if err != nil {
			return node, err
		}

		node.Add(n)

		tok := p.scan()
		if tok.Type != token.COMMA {
			p.unscan()
		}
	}
	return node, nil
}

func (p *Parser) consumeComment() (comment *ast.Comment, endline int) {
	endline = p.tok.Pos.Line

	if len(p.tok.Text) > 1 && p.tok.Text[1] == '*' {

		for i := 0; i < len(p.tok.Text); i++ {
			if p.tok.Text[i] == '\n' {
				endline++
			}
		}
	}

	comment = &ast.Comment{Start: p.tok.Pos, Text: p.tok.Text}
	p.tok = p.sc.Scan()
	return
}

func (p *Parser) consumeCommentGroup(n int) (comments *ast.CommentGroup, endline int) {
	var list []*ast.Comment
	endline = p.tok.Pos.Line

	for p.tok.Type == token.COMMENT && p.tok.Pos.Line <= endline+n {
		var comment *ast.Comment
		comment, endline = p.consumeComment()
		list = append(list, comment)
	}

	comments = &ast.CommentGroup{List: list}
	p.comments = append(p.comments, comments)

	return
}

func (p *Parser) objectItem() (*ast.ObjectItem, error) {
	defer un(trace(p, "ParseObjectItem"))

	keys, err := p.objectKey()
	if len(keys) > 0 && err == errEofToken {

		err = nil
	}
	if len(keys) > 0 && err != nil && p.tok.Type == token.RBRACE {

		err = nil

		p.tok.Type = token.EOF
	}
	if err != nil {
		return nil, err
	}

	o := &ast.ObjectItem{
		Keys: keys,
	}

	if p.leadComment != nil {
		o.LeadComment = p.leadComment
		p.leadComment = nil
	}

	switch p.tok.Type {
	case token.ASSIGN:
		o.Assign = p.tok.Pos
		o.Val, err = p.object()
		if err != nil {
			return nil, err
		}
	case token.LBRACE:
		o.Val, err = p.objectType()
		if err != nil {
			return nil, err
		}
	default:
		keyStr := make([]string, 0, len(keys))
		for _, k := range keys {
			keyStr = append(keyStr, k.Token.Text)
		}

		return nil, fmt.Errorf(
			"key '%s' expected start of object ('{') or assignment ('=')",
			strings.Join(keyStr, " "))
	}

	p.scan()
	if len(keys) > 0 && o.Val.Pos().Line == keys[0].Pos().Line && p.lineComment != nil {
		o.LineComment = p.lineComment
		p.lineComment = nil
	}
	p.unscan()
	return o, nil
}

func (p *Parser) objectKey() ([]*ast.ObjectKey, error) {
	keyCount := 0
	keys := make([]*ast.ObjectKey, 0)

	for {
		tok := p.scan()
		switch tok.Type {
		case token.EOF:

			return keys, errEofToken
		case token.ASSIGN:

			if keyCount > 1 {
				return nil, &PosError{
					Pos: p.tok.Pos,
					Err: fmt.Errorf("nested object expected: LBRACE got: %s", p.tok.Type),
				}
			}

			if keyCount == 0 {
				return nil, &PosError{
					Pos: p.tok.Pos,
					Err: errors.New("no object keys found!"),
				}
			}

			return keys, nil
		case token.LBRACE:
			var err error

			if len(keys) == 0 {
				err = &PosError{
					Pos: p.tok.Pos,
					Err: fmt.Errorf("expected: IDENT | STRING got: %s", p.tok.Type),
				}
			}

			return keys, err
		case token.IDENT, token.STRING:
			keyCount++
			keys = append(keys, &ast.ObjectKey{Token: p.tok})
		case token.ILLEGAL:
			fmt.Println("illegal")
		default:
			return keys, &PosError{
				Pos: p.tok.Pos,
				Err: fmt.Errorf("expected: IDENT | STRING | ASSIGN | LBRACE got: %s", p.tok.Type),
			}
		}
	}
}

func (p *Parser) object() (ast.Node, error) {
	defer un(trace(p, "ParseType"))
	tok := p.scan()

	switch tok.Type {
	case token.NUMBER, token.FLOAT, token.BOOL, token.STRING, token.HEREDOC:
		return p.literalType()
	case token.LBRACE:
		return p.objectType()
	case token.LBRACK:
		return p.listType()
	case token.COMMENT:

	case token.EOF:
		return nil, errEofToken
	}

	return nil, &PosError{
		Pos: tok.Pos,
		Err: fmt.Errorf("Unknown token: %+v", tok),
	}
}

func (p *Parser) objectType() (*ast.ObjectType, error) {
	defer un(trace(p, "ParseObjectType"))

	o := &ast.ObjectType{
		Lbrace: p.tok.Pos,
	}

	l, err := p.objectList()

	if err != nil && p.tok.Type != token.RBRACE {
		return nil, err
	}

	if p.tok.Type != token.RBRACE {
		return nil, fmt.Errorf("object expected closing RBRACE got: %s", p.tok.Type)
	}

	o.List = l
	o.Rbrace = p.tok.Pos // advanced via parseObjectList
	return o, nil
}

func (p *Parser) listType() (*ast.ListType, error) {
	defer un(trace(p, "ParseListType"))

	l := &ast.ListType{
		Lbrack: p.tok.Pos,
	}

	needComma := false
	for {
		tok := p.scan()
		if needComma {
			switch tok.Type {
			case token.COMMA, token.RBRACK:
			default:
				return nil, &PosError{
					Pos: tok.Pos,
					Err: fmt.Errorf(
						"error parsing list, expected comma or list end, got: %s",
						tok.Type),
				}
			}
		}
		switch tok.Type {
		case token.NUMBER, token.FLOAT, token.STRING, token.HEREDOC:
			node, err := p.literalType()
			if err != nil {
				return nil, err
			}

			l.Add(node)
			needComma = true
		case token.COMMA:

			p.scan()
			if p.lineComment != nil && len(l.List) > 0 {
				lit, ok := l.List[len(l.List)-1].(*ast.LiteralType)
				if ok {
					lit.LineComment = p.lineComment
					l.List[len(l.List)-1] = lit
					p.lineComment = nil
				}
			}
			p.unscan()

			needComma = false
			continue
		case token.LBRACE:

			node, err := p.objectType()
			if err != nil {
				return nil, &PosError{
					Pos: tok.Pos,
					Err: fmt.Errorf(
						"error while trying to parse object within list: %s", err),
				}
			}
			l.Add(node)
			needComma = true
		case token.BOOL:

		case token.LBRACK:

		case token.RBRACK:

			l.Rbrack = p.tok.Pos
			return l, nil
		default:
			return nil, &PosError{
				Pos: tok.Pos,
				Err: fmt.Errorf("unexpected token while parsing list: %s", tok.Type),
			}
		}
	}
}

func (p *Parser) literalType() (*ast.LiteralType, error) {
	defer un(trace(p, "ParseLiteral"))

	return &ast.LiteralType{
		Token: p.tok,
	}, nil
}

func (p *Parser) scan() token.Token {

	if p.n != 0 {
		p.n = 0
		return p.tok
	}

	prev := p.tok
	p.tok = p.sc.Scan()

	if p.tok.Type == token.COMMENT {
		var comment *ast.CommentGroup
		var endline int

		if p.tok.Pos.Line == prev.Pos.Line {

			comment, endline = p.consumeCommentGroup(0)
			if p.tok.Pos.Line != endline {

				p.lineComment = comment
			}
		}

		endline = -1
		for p.tok.Type == token.COMMENT {
			comment, endline = p.consumeCommentGroup(1)
		}

		if endline+1 == p.tok.Pos.Line && p.tok.Type != token.RBRACE {
			switch p.tok.Type {
			case token.RBRACE, token.RBRACK:

			default:

				p.leadComment = comment
			}
		}

	}

	return p.tok
}

func (p *Parser) unscan() {
	p.n = 1
}

func (p *Parser) printTrace(a ...interface{}) {
	if !p.enableTrace {
		return
	}

	const dots = ". . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . "
	const n = len(dots)
	fmt.Printf("%5d:%3d: ", p.tok.Pos.Line, p.tok.Pos.Column)

	i := 2 * p.indent
	for i > n {
		fmt.Print(dots)
		i -= n
	}

	fmt.Print(dots[0:i])
	fmt.Println(a...)
}

func trace(p *Parser, msg string) *Parser {
	p.printTrace(msg, "(")
	p.indent++
	return p
}

func un(p *Parser) {
	p.indent--
	p.printTrace(")")
}
