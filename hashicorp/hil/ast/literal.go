package ast

import (
	"fmt"
)

type LiteralNode struct {
	Value interface{}
	Typex Type
	Posx  Pos
}

func (n *LiteralNode) Accept(v Visitor) Node {
	return v(n)
}

func (n *LiteralNode) Pos() Pos {
	return n.Posx
}

func (n *LiteralNode) GoString() string {
	return fmt.Sprintf("*%#v", *n)
}

func (n *LiteralNode) String() string {
	return fmt.Sprintf("Literal(%s, %v)", n.Typex, n.Value)
}

func (n *LiteralNode) Type(Scope) (Type, error) {
	return n.Typex, nil
}
