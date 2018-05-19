package hil

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/hashicorp/hil/ast"
)

type EvalConfig struct {
	GlobalScope *ast.BasicScope

	SemanticChecks []SemanticChecker
}

type SemanticChecker func(ast.Node) error

type EvalType uint32

const (
	TypeInvalid EvalType = 0
	TypeString  EvalType = 1 << iota
	TypeList
	TypeMap
)

type EvaluationResult struct {
	Type  EvalType
	Value interface{}
}

var InvalidResult = EvaluationResult{Type: TypeInvalid, Value: nil}

func Eval(root ast.Node, config *EvalConfig) (EvaluationResult, error) {
	output, outputType, err := internalEval(root, config)
	if err != nil {
		return InvalidResult, err
	}

	switch outputType {
	case ast.TypeList:
		val, err := VariableToInterface(ast.Variable{
			Type:  ast.TypeList,
			Value: output,
		})
		return EvaluationResult{
			Type:  TypeList,
			Value: val,
		}, err
	case ast.TypeMap:
		val, err := VariableToInterface(ast.Variable{
			Type:  ast.TypeMap,
			Value: output,
		})
		return EvaluationResult{
			Type:  TypeMap,
			Value: val,
		}, err
	case ast.TypeString:
		return EvaluationResult{
			Type:  TypeString,
			Value: output,
		}, nil
	default:
		return InvalidResult, fmt.Errorf("unknown type %s as interpolation output", outputType)
	}
}

func internalEval(root ast.Node, config *EvalConfig) (interface{}, ast.Type, error) {

	if config == nil {
		config = new(EvalConfig)
	}
	scope := registerBuiltins(config.GlobalScope)
	implicitMap := map[ast.Type]map[ast.Type]string{
		ast.TypeFloat: {
			ast.TypeInt:    "__builtin_FloatToInt",
			ast.TypeString: "__builtin_FloatToString",
		},
		ast.TypeInt: {
			ast.TypeFloat:  "__builtin_IntToFloat",
			ast.TypeString: "__builtin_IntToString",
		},
		ast.TypeString: {
			ast.TypeInt:   "__builtin_StringToInt",
			ast.TypeFloat: "__builtin_StringToFloat",
		},
	}

	tv := &TypeCheck{Scope: scope, Implicit: implicitMap}
	ic := &IdentifierCheck{Scope: scope}

	checks := make(
		[]SemanticChecker,
		len(config.SemanticChecks),
		len(config.SemanticChecks)+2)
	copy(checks, config.SemanticChecks)
	checks = append(checks, ic.Visit)
	checks = append(checks, tv.Visit)

	for _, check := range checks {
		if err := check(root); err != nil {
			return nil, ast.TypeInvalid, err
		}
	}

	v := &evalVisitor{Scope: scope}
	return v.Visit(root)
}

// stack as a LiteralNode. Pop elements off the stack to get child
type EvalNode interface {
	Eval(ast.Scope, *ast.Stack) (interface{}, ast.Type, error)
}

type evalVisitor struct {
	Scope ast.Scope
	Stack ast.Stack

	err  error
	lock sync.Mutex
}

func (v *evalVisitor) Visit(root ast.Node) (interface{}, ast.Type, error) {

	root.Accept(v.visit)

	var result *ast.LiteralNode
	if v.Stack.Len() > 0 {
		result = v.Stack.Pop().(*ast.LiteralNode)
	} else {
		result = new(ast.LiteralNode)
	}
	resultErr := v.err

	v.Stack.Reset()
	v.err = nil

	t, err := result.Type(v.Scope)
	if err != nil {
		return nil, ast.TypeInvalid, err
	}

	return result.Value, t, resultErr
}

func (v *evalVisitor) visit(raw ast.Node) ast.Node {
	if v.err != nil {
		return raw
	}

	en, err := evalNode(raw)
	if err != nil {
		v.err = err
		return raw
	}

	out, outType, err := en.Eval(v.Scope, &v.Stack)
	if err != nil {
		v.err = err
		return raw
	}

	v.Stack.Push(&ast.LiteralNode{
		Value: out,
		Typex: outType,
	})
	return raw
}

func evalNode(raw ast.Node) (EvalNode, error) {
	switch n := raw.(type) {
	case *ast.Index:
		return &evalIndex{n}, nil
	case *ast.Call:
		return &evalCall{n}, nil
	case *ast.Output:
		return &evalOutput{n}, nil
	case *ast.LiteralNode:
		return &evalLiteralNode{n}, nil
	case *ast.VariableAccess:
		return &evalVariableAccess{n}, nil
	default:
		en, ok := n.(EvalNode)
		if !ok {
			return nil, fmt.Errorf("node doesn't support evaluation: %#v", raw)
		}

		return en, nil
	}
}

type evalCall struct{ *ast.Call }

func (v *evalCall) Eval(s ast.Scope, stack *ast.Stack) (interface{}, ast.Type, error) {

	function, ok := s.LookupFunc(v.Func)
	if !ok {
		return nil, ast.TypeInvalid, fmt.Errorf(
			"unknown function called: %s", v.Func)
	}

	args := make([]interface{}, len(v.Args))
	for i := range v.Args {
		node := stack.Pop().(*ast.LiteralNode)
		args[len(v.Args)-1-i] = node.Value
	}

	result, err := function.Callback(args)
	if err != nil {
		return nil, ast.TypeInvalid, fmt.Errorf("%s: %s", v.Func, err)
	}

	return result, function.ReturnType, nil
}

type evalIndex struct{ *ast.Index }

func (v *evalIndex) Eval(scope ast.Scope, stack *ast.Stack) (interface{}, ast.Type, error) {
	evalVarAccess, err := evalNode(v.Target)
	if err != nil {
		return nil, ast.TypeInvalid, err
	}
	target, targetType, err := evalVarAccess.Eval(scope, stack)

	evalKey, err := evalNode(v.Key)
	if err != nil {
		return nil, ast.TypeInvalid, err
	}

	key, keyType, err := evalKey.Eval(scope, stack)
	if err != nil {
		return nil, ast.TypeInvalid, err
	}

	variableName := v.Index.Target.(*ast.VariableAccess).Name

	switch targetType {
	case ast.TypeList:
		if keyType != ast.TypeInt {
			return nil, ast.TypeInvalid, fmt.Errorf("key for indexing list %q must be an int, is %s", variableName, keyType)
		}

		return v.evalListIndex(variableName, target, key)
	case ast.TypeMap:
		if keyType != ast.TypeString {
			return nil, ast.TypeInvalid, fmt.Errorf("key for indexing map %q must be a string, is %s", variableName, keyType)
		}

		return v.evalMapIndex(variableName, target, key)
	default:
		return nil, ast.TypeInvalid, fmt.Errorf("target %q for indexing must be ast.TypeList or ast.TypeMap, is %s", variableName, targetType)
	}
}

func (v *evalIndex) evalListIndex(variableName string, target interface{}, key interface{}) (interface{}, ast.Type, error) {

	list, ok := target.([]ast.Variable)
	if !ok {
		return nil, ast.TypeInvalid, fmt.Errorf("cannot cast target to []Variable")
	}

	keyInt, ok := key.(int)
	if !ok {
		return nil, ast.TypeInvalid, fmt.Errorf("cannot cast key to int")
	}

	if len(list) == 0 {
		return nil, ast.TypeInvalid, fmt.Errorf("list is empty")
	}

	if keyInt < 0 || len(list) < keyInt+1 {
		return nil, ast.TypeInvalid, fmt.Errorf("index %d out of range for list %s (max %d)", keyInt, variableName, len(list))
	}

	returnVal := list[keyInt].Value
	returnType := list[keyInt].Type

	return returnVal, returnType, nil
}

func (v *evalIndex) evalMapIndex(variableName string, target interface{}, key interface{}) (interface{}, ast.Type, error) {

	vmap, ok := target.(map[string]ast.Variable)
	if !ok {
		return nil, ast.TypeInvalid, fmt.Errorf("cannot cast target to map[string]Variable")
	}

	keyString, ok := key.(string)
	if !ok {
		return nil, ast.TypeInvalid, fmt.Errorf("cannot cast key to string")
	}

	if len(vmap) == 0 {
		return nil, ast.TypeInvalid, fmt.Errorf("map is empty")
	}

	value, ok := vmap[keyString]
	if !ok {
		return nil, ast.TypeInvalid, fmt.Errorf("key %q does not exist in map %s", keyString, variableName)
	}

	return value.Value, value.Type, nil
}

type evalOutput struct{ *ast.Output }

func (v *evalOutput) Eval(s ast.Scope, stack *ast.Stack) (interface{}, ast.Type, error) {

	nodes := make([]*ast.LiteralNode, 0, len(v.Exprs))
	for range v.Exprs {
		nodes = append(nodes, stack.Pop().(*ast.LiteralNode))
	}

	if len(nodes) == 1 && nodes[0].Typex == ast.TypeList {
		return nodes[0].Value, ast.TypeList, nil
	}
	if len(nodes) == 1 && nodes[0].Typex == ast.TypeMap {
		return nodes[0].Value, ast.TypeMap, nil
	}

	var buf bytes.Buffer
	for i := len(nodes) - 1; i >= 0; i-- {
		buf.WriteString(nodes[i].Value.(string))
	}

	return buf.String(), ast.TypeString, nil
}

type evalLiteralNode struct{ *ast.LiteralNode }

func (v *evalLiteralNode) Eval(ast.Scope, *ast.Stack) (interface{}, ast.Type, error) {
	return v.Value, v.Typex, nil
}

type evalVariableAccess struct{ *ast.VariableAccess }

func (v *evalVariableAccess) Eval(scope ast.Scope, _ *ast.Stack) (interface{}, ast.Type, error) {

	variable, ok := scope.LookupVar(v.Name)
	if !ok {
		return nil, ast.TypeInvalid, fmt.Errorf(
			"unknown variable accessed: %s", v.Name)
	}

	return variable.Value, variable.Type, nil
}
