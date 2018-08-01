package ast

import (
	"fmt"
	"reflect"
)

type Scope interface {
	LookupFunc(string) (Function, bool)
	LookupVar(string) (Variable, bool)
}

type Variable struct {
	Value interface{}
	Type  Type
}

func NewVariable(v interface{}) (result Variable, err error) {
	switch v := reflect.ValueOf(v); v.Kind() {
	case reflect.String:
		result.Type = TypeString
	default:
		err = fmt.Errorf("Unknown type: %s", v.Kind())
	}

	result.Value = v
	return
}

func (v Variable) String() string {
	return fmt.Sprintf("{Variable (%s): %+v}", v.Type, v.Value)
}

type Function struct {

	//

	ArgTypes   []Type
	ReturnType Type

	Variadic     bool
	VariadicType Type

	Callback func([]interface{}) (interface{}, error)
}

type BasicScope struct {
	FuncMap map[string]Function
	VarMap  map[string]Variable
}

func (s *BasicScope) LookupFunc(n string) (Function, bool) {
	if s == nil {
		return Function{}, false
	}

	v, ok := s.FuncMap[n]
	return v, ok
}

func (s *BasicScope) LookupVar(n string) (Variable, bool) {
	if s == nil {
		return Variable{}, false
	}

	v, ok := s.VarMap[n]
	return v, ok
}
