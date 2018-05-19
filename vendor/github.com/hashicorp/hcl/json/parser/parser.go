package parser

import (
	"errors"
	"fmt"

	"github.com/hashicorp/hcl/hcl/ast"
	"github.com/hashicorp/hcl/json/scanner"
	"github.com/hashicorp/hcl/json/token"
)

type Parser struct {
	sc *scanner.Scanner

	tok       token.Token
	commaPrev token.Token

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
		scerr = fmt.Errorf("%s: %s", pos, msg)
	}

	object, err := p.object()
	if scerr != nil {
		return nil, scerr
	}
	if err != nil {
		return nil, err
	}

	f.Node = object.List

	flattenObjects(f.Node)

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

		if tok := p.scan(); tok.Type != token.COMMA {
			break
		}
	}
	return node, nil
}

func (p *Parser) objectItem() (*ast.ObjectItem, error) {
	defer un(trace(p, "ParseObjectItem"))

	keys, err := p.objectKey()
	if err != nil {
		return nil, err
	}

	o := &ast.ObjectItem{
		Keys: keys,
	}

	switch p.tok.Type {
	case token.COLON:
		o.Val, err = p.objectValue()
		if err != nil {
			return nil, err
		}
	}

	return o, nil
}

func (p *Parser) objectKey() ([]*ast.ObjectKey, error) {
	keyCount := 0
	keys := make([]*ast.ObjectKey, 0)

	for {
		tok := p.scan()
		switch tok.Type {
		case token.EOF:
			return nil, errEofToken
		case token.STRING:
			keyCount++
			keys = append(keys, &ast.ObjectKey{
				Token: p.tok.HCLToken(),
			})
		case token.COLON:

			if keyCount == 0 {
				return nil, fmt.Errorf("expected: STRING got: %s", p.tok.Type)
			}

			return keys, nil
		case token.ILLEGAL:
			fmt.Println("illegal")
		default:
			return nil, fmt.Errorf("expected: STRING got: %s", p.tok.Type)
		}
	}
}

func (p *Parser) objectValue() (ast.Node, error) {
	defer un(trace(p, "ParseObjectValue"))
	tok := p.scan()

	switch tok.Type {
	case token.NUMBER, token.FLOAT, token.BOOL, token.NULL, token.STRING:
		return p.literalType()
	case token.LBRACE:
		return p.objectType()
	case token.LBRACK:
		return p.listType()
	case token.EOF:
		return nil, errEofToken
	}

	return nil, fmt.Errorf("Expected object value, got unknown token: %+v", tok)
}

func (p *Parser) object() (*ast.ObjectType, error) {
	defer un(trace(p, "ParseType"))
	tok := p.scan()

	switch tok.Type {
	case token.LBRACE:
		return p.objectType()
	case token.EOF:
		return nil, errEofToken
	}

	return nil, fmt.Errorf("Expected object, got unknown token: %+v", tok)
}

func (p *Parser) objectType() (*ast.ObjectType, error) {
	defer un(trace(p, "ParseObjectType"))

	o := &ast.ObjectType{}

	l, err := p.objectList()

	if err != nil && p.tok.Type != token.RBRACE {
		return nil, err
	}

	o.List = l
	return o, nil
}

func (p *Parser) listType() (*ast.ListType, error) {
	defer un(trace(p, "ParseListType"))

	l := &ast.ListType{}

	for {
		tok := p.scan()
		switch tok.Type {
		case token.NUMBER, token.FLOAT, token.STRING:
			node, err := p.literalType()
			if err != nil {
				return nil, err
			}

			l.Add(node)
		case token.COMMA:
			continue
		case token.LBRACE:
			node, err := p.objectType()
			if err != nil {
				return nil, err
			}

			l.Add(node)
		case token.BOOL:

		case token.LBRACK:

		case token.RBRACK:

			return l, nil
		default:
			return nil, fmt.Errorf("unexpected token while parsing list: %s", tok.Type)
		}

	}
}

func (p *Parser) literalType() (*ast.LiteralType, error) {
	defer un(trace(p, "ParseLiteral"))

	return &ast.LiteralType{
		Token: p.tok.HCLToken(),
	}, nil
}

func (p *Parser) scan() token.Token {

	if p.n != 0 {
		p.n = 0
		return p.tok
	}

	p.tok = p.sc.Scan()
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
