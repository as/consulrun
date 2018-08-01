package hil

import (
	"github.com/as/consulrun/hashicorp/hil/ast"
)

//
func FixedValueTransform(root ast.Node, Value *ast.LiteralNode) ast.Node {

	result := root
	switch n := result.(type) {
	case *ast.Output:
		for i, v := range n.Exprs {
			n.Exprs[i] = FixedValueTransform(v, Value)
		}
	case *ast.LiteralNode:

	default:

		result = Value
	}

	return result
}
