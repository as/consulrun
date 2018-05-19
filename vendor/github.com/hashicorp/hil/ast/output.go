package ast

import (
	"bytes"
	"fmt"
)

type Output struct {
	Exprs []Node
	Posx  Pos
}

func (n *Output) Accept(v Visitor) Node {
	for i, expr := range n.Exprs {
		n.Exprs[i] = expr.Accept(v)
	}

	return v(n)
}

func (n *Output) Pos() Pos {
	return n.Posx
}

func (n *Output) GoString() string {
	return fmt.Sprintf("*%#v", *n)
}

func (n *Output) String() string {
	var b bytes.Buffer
	for _, expr := range n.Exprs {
		b.WriteString(fmt.Sprintf("%s", expr))
	}

	return b.String()
}

func (n *Output) Type(s Scope) (Type, error) {

	if len(n.Exprs) == 0 {
		return TypeString, nil
	}

	if len(n.Exprs) == 1 {
		exprType, err := n.Exprs[0].Type(s)
		if err != nil {
			return TypeInvalid, err
		}
		switch exprType {
		case TypeList:
			return TypeList, nil
		case TypeMap:
			return TypeMap, nil
		}
	}

	for index, expr := range n.Exprs {
		exprType, err := expr.Type(s)
		if err != nil {
			return TypeInvalid, err
		}

		if exprType == TypeList || exprType == TypeMap {
			return TypeInvalid, fmt.Errorf(
				"multi-expression HIL outputs may only have string inputs: %d is type %s",
				index, exprType)
		}
	}

	return TypeString, nil
}
