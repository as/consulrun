package hil

import (
	"fmt"
	"sync"

	"github.com/as/consulrun/hashicorp/hil/ast"
)

type IdentifierCheck struct {
	Scope ast.Scope

	err  error
	lock sync.Mutex
}

func (c *IdentifierCheck) Visit(root ast.Node) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	defer c.reset()
	root.Accept(c.visit)
	return c.err
}

func (c *IdentifierCheck) visit(raw ast.Node) ast.Node {
	if c.err != nil {
		return raw
	}

	switch n := raw.(type) {
	case *ast.Call:
		c.visitCall(n)
	case *ast.VariableAccess:
		c.visitVariableAccess(n)
	case *ast.Output:

	case *ast.LiteralNode:

	default:

	}

	return raw
}

func (c *IdentifierCheck) visitCall(n *ast.Call) {

	function, ok := c.Scope.LookupFunc(n.Func)
	if !ok {
		c.createErr(n, fmt.Sprintf("unknown function called: %s", n.Func))
		return
	}

	args := n.Args
	if function.Variadic && len(args) > len(function.ArgTypes) {
		args = n.Args[:len(function.ArgTypes)]
	}

	if len(args) != len(function.ArgTypes) {
		c.createErr(n, fmt.Sprintf(
			"%s: expected %d arguments, got %d",
			n.Func, len(function.ArgTypes), len(n.Args)))
		return
	}
}

func (c *IdentifierCheck) visitVariableAccess(n *ast.VariableAccess) {

	if _, ok := c.Scope.LookupVar(n.Name); !ok {
		c.createErr(n, fmt.Sprintf(
			"unknown variable accessed: %s", n.Name))
		return
	}
}

func (c *IdentifierCheck) createErr(n ast.Node, str string) {
	c.err = fmt.Errorf("%s: %s", n.Pos(), str)
}

func (c *IdentifierCheck) reset() {
	c.err = nil
}
