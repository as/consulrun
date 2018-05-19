package hil

import (
	"fmt"
	"sync"

	"github.com/hashicorp/hil/ast"
)

//
type TypeCheck struct {
	Scope ast.Scope

	Implicit map[ast.Type]map[ast.Type]string

	Stack []ast.Type

	err  error
	lock sync.Mutex
}

type TypeCheckNode interface {
	TypeCheck(*TypeCheck) (ast.Node, error)
}

func (v *TypeCheck) Visit(root ast.Node) error {
	v.lock.Lock()
	defer v.lock.Unlock()
	defer v.reset()
	root.Accept(v.visit)
	return v.err
}

func (v *TypeCheck) visit(raw ast.Node) ast.Node {
	if v.err != nil {
		return raw
	}

	var result ast.Node
	var err error
	switch n := raw.(type) {
	case *ast.Arithmetic:
		tc := &typeCheckArithmetic{n}
		result, err = tc.TypeCheck(v)
	case *ast.Call:
		tc := &typeCheckCall{n}
		result, err = tc.TypeCheck(v)
	case *ast.Index:
		tc := &typeCheckIndex{n}
		result, err = tc.TypeCheck(v)
	case *ast.Output:
		tc := &typeCheckOutput{n}
		result, err = tc.TypeCheck(v)
	case *ast.LiteralNode:
		tc := &typeCheckLiteral{n}
		result, err = tc.TypeCheck(v)
	case *ast.VariableAccess:
		tc := &typeCheckVariableAccess{n}
		result, err = tc.TypeCheck(v)
	default:
		tc, ok := raw.(TypeCheckNode)
		if !ok {
			err = fmt.Errorf("unknown node for type check: %#v", raw)
			break
		}

		result, err = tc.TypeCheck(v)
	}

	if err != nil {
		pos := raw.Pos()
		v.err = fmt.Errorf("At column %d, line %d: %s",
			pos.Column, pos.Line, err)
	}

	return result
}

type typeCheckArithmetic struct {
	n *ast.Arithmetic
}

func (tc *typeCheckArithmetic) TypeCheck(v *TypeCheck) (ast.Node, error) {

	exprs := make([]ast.Type, len(tc.n.Exprs))
	for i := range tc.n.Exprs {
		exprs[len(tc.n.Exprs)-1-i] = v.StackPop()
	}

	mathFunc := "__builtin_IntMath"
	mathType := ast.TypeInt
	for _, v := range exprs {
		exit := true
		switch v {
		case ast.TypeInt:
			mathFunc = "__builtin_IntMath"
			mathType = v
		case ast.TypeFloat:
			mathFunc = "__builtin_FloatMath"
			mathType = v
		default:
			exit = false
		}

		if exit {
			break
		}
	}

	for i, arg := range exprs {
		if arg != mathType {
			cn := v.ImplicitConversion(exprs[i], mathType, tc.n.Exprs[i])
			if cn != nil {
				tc.n.Exprs[i] = cn
				continue
			}

			return nil, fmt.Errorf(
				"operand %d should be %s, got %s",
				i+1, mathType, arg)
		}
	}

	if mathType == ast.TypeFloat && tc.n.Op == ast.ArithmeticOpMod {
		return nil, fmt.Errorf("modulo cannot be used with floats")
	}

	v.StackPush(mathType)

	args := make([]ast.Node, len(tc.n.Exprs)+1)
	args[0] = &ast.LiteralNode{
		Value: tc.n.Op,
		Typex: ast.TypeInt,
		Posx:  tc.n.Pos(),
	}
	copy(args[1:], tc.n.Exprs)
	return &ast.Call{
		Func: mathFunc,
		Args: args,
		Posx: tc.n.Pos(),
	}, nil
}

type typeCheckCall struct {
	n *ast.Call
}

func (tc *typeCheckCall) TypeCheck(v *TypeCheck) (ast.Node, error) {

	function, ok := v.Scope.LookupFunc(tc.n.Func)
	if !ok {
		return nil, fmt.Errorf("unknown function called: %s", tc.n.Func)
	}

	args := make([]ast.Type, len(tc.n.Args))
	for i := range tc.n.Args {
		args[len(tc.n.Args)-1-i] = v.StackPop()
	}

	for i, expected := range function.ArgTypes {
		if expected == ast.TypeAny {
			continue
		}

		if args[i] != expected {
			cn := v.ImplicitConversion(args[i], expected, tc.n.Args[i])
			if cn != nil {
				tc.n.Args[i] = cn
				continue
			}

			return nil, fmt.Errorf(
				"%s: argument %d should be %s, got %s",
				tc.n.Func, i+1, expected.Printable(), args[i].Printable())
		}
	}

	if function.Variadic && function.VariadicType != ast.TypeAny {
		args = args[len(function.ArgTypes):]
		for i, t := range args {
			if t != function.VariadicType {
				realI := i + len(function.ArgTypes)
				cn := v.ImplicitConversion(
					t, function.VariadicType, tc.n.Args[realI])
				if cn != nil {
					tc.n.Args[realI] = cn
					continue
				}

				return nil, fmt.Errorf(
					"%s: argument %d should be %s, got %s",
					tc.n.Func, realI,
					function.VariadicType.Printable(), t.Printable())
			}
		}
	}

	v.StackPush(function.ReturnType)

	return tc.n, nil
}

type typeCheckOutput struct {
	n *ast.Output
}

func (tc *typeCheckOutput) TypeCheck(v *TypeCheck) (ast.Node, error) {
	n := tc.n
	types := make([]ast.Type, len(n.Exprs))
	for i := range n.Exprs {
		types[len(n.Exprs)-1-i] = v.StackPop()
	}

	if len(types) == 1 && types[0] == ast.TypeList {
		v.StackPush(ast.TypeList)
		return n, nil
	}

	if len(types) == 1 && types[0] == ast.TypeMap {
		v.StackPush(ast.TypeMap)
		return n, nil
	}

	for i, t := range types {
		if t != ast.TypeString {
			cn := v.ImplicitConversion(t, ast.TypeString, n.Exprs[i])
			if cn != nil {
				n.Exprs[i] = cn
				continue
			}

			return nil, fmt.Errorf(
				"output of an HIL expression must be a string, or a single list (argument %d is %s)", i+1, t)
		}
	}

	v.StackPush(ast.TypeString)

	return n, nil
}

type typeCheckLiteral struct {
	n *ast.LiteralNode
}

func (tc *typeCheckLiteral) TypeCheck(v *TypeCheck) (ast.Node, error) {
	v.StackPush(tc.n.Typex)
	return tc.n, nil
}

type typeCheckVariableAccess struct {
	n *ast.VariableAccess
}

func (tc *typeCheckVariableAccess) TypeCheck(v *TypeCheck) (ast.Node, error) {

	variable, ok := v.Scope.LookupVar(tc.n.Name)
	if !ok {
		return nil, fmt.Errorf(
			"unknown variable accessed: %s", tc.n.Name)
	}

	v.StackPush(variable.Type)

	return tc.n, nil
}

type typeCheckIndex struct {
	n *ast.Index
}

func (tc *typeCheckIndex) TypeCheck(v *TypeCheck) (ast.Node, error) {

	varAccessNode, ok := tc.n.Target.(*ast.VariableAccess)
	if !ok {
		return nil, fmt.Errorf("target of an index must be a VariableAccess node, was %T", tc.n.Target)
	}

	variable, ok := v.Scope.LookupVar(varAccessNode.Name)
	if !ok {
		return nil, fmt.Errorf("unknown variable accessed: %s", varAccessNode.Name)
	}

	keyType, err := tc.n.Key.Type(v.Scope)
	if err != nil {
		return nil, err
	}

	switch variable.Type {
	case ast.TypeList:
		if keyType != ast.TypeInt {
			return nil, fmt.Errorf("key of an index must be an int, was %s", keyType)
		}

		valType, err := ast.VariableListElementTypesAreHomogenous(varAccessNode.Name, variable.Value.([]ast.Variable))
		if err != nil {
			return tc.n, err
		}

		v.StackPush(valType)
		return tc.n, nil
	case ast.TypeMap:
		if keyType != ast.TypeString {
			return nil, fmt.Errorf("key of an index must be a string, was %s", keyType)
		}

		valType, err := ast.VariableMapValueTypesAreHomogenous(varAccessNode.Name, variable.Value.(map[string]ast.Variable))
		if err != nil {
			return tc.n, err
		}

		v.StackPush(valType)
		return tc.n, nil
	default:
		return nil, fmt.Errorf("invalid index operation into non-indexable type: %s", variable.Type)
	}
}

func (v *TypeCheck) ImplicitConversion(
	actual ast.Type, expected ast.Type, n ast.Node) ast.Node {
	if v.Implicit == nil {
		return nil
	}

	fromMap, ok := v.Implicit[actual]
	if !ok {
		return nil
	}

	toFunc, ok := fromMap[expected]
	if !ok {
		return nil
	}

	return &ast.Call{
		Func: toFunc,
		Args: []ast.Node{n},
		Posx: n.Pos(),
	}
}

func (v *TypeCheck) reset() {
	v.Stack = nil
	v.err = nil
}

func (v *TypeCheck) StackPush(t ast.Type) {
	v.Stack = append(v.Stack, t)
}

func (v *TypeCheck) StackPop() ast.Type {
	var x ast.Type
	x, v.Stack = v.Stack[len(v.Stack)-1], v.Stack[:len(v.Stack)-1]
	return x
}
