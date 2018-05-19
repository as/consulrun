package hil

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/hashicorp/hil/ast"
	"github.com/mitchellh/reflectwalk"
)

type WalkFn func(*WalkData) error

//
type WalkData struct {
	Root ast.Node

	Location reflectwalk.Location

	//

	Replace      bool
	ReplaceValue string
}

//
func Walk(v interface{}, cb WalkFn) error {
	walker := &interpolationWalker{F: cb}
	return reflectwalk.Walk(v, walker)
}

type interpolationWalker struct {
	F WalkFn

	key         []string
	lastValue   reflect.Value
	loc         reflectwalk.Location
	cs          []reflect.Value
	csKey       []reflect.Value
	csData      interface{}
	sliceIndex  int
	unknownKeys []string
}

func (w *interpolationWalker) Enter(loc reflectwalk.Location) error {
	w.loc = loc
	return nil
}

func (w *interpolationWalker) Exit(loc reflectwalk.Location) error {
	w.loc = reflectwalk.None

	switch loc {
	case reflectwalk.Map:
		w.cs = w.cs[:len(w.cs)-1]
	case reflectwalk.MapValue:
		w.key = w.key[:len(w.key)-1]
		w.csKey = w.csKey[:len(w.csKey)-1]
	case reflectwalk.Slice:

		w.splitSlice()
		w.cs = w.cs[:len(w.cs)-1]
	case reflectwalk.SliceElem:
		w.csKey = w.csKey[:len(w.csKey)-1]
	}

	return nil
}

func (w *interpolationWalker) Map(m reflect.Value) error {
	w.cs = append(w.cs, m)
	return nil
}

func (w *interpolationWalker) MapElem(m, k, v reflect.Value) error {
	w.csData = k
	w.csKey = append(w.csKey, k)
	w.key = append(w.key, k.String())
	w.lastValue = v
	return nil
}

func (w *interpolationWalker) Slice(s reflect.Value) error {
	w.cs = append(w.cs, s)
	return nil
}

func (w *interpolationWalker) SliceElem(i int, elem reflect.Value) error {
	w.csKey = append(w.csKey, reflect.ValueOf(i))
	w.sliceIndex = i
	return nil
}

func (w *interpolationWalker) Primitive(v reflect.Value) error {
	setV := v

	if v.Kind() == reflect.Interface {
		setV = v
		v = v.Elem()
	}
	if v.Kind() != reflect.String {
		return nil
	}

	astRoot, err := Parse(v.String())
	if err != nil {
		return err
	}

	if n, ok := astRoot.(*ast.LiteralNode); ok {
		if s, ok := n.Value.(string); ok && s == v.String() {
			return nil
		}
	}

	if w.F == nil {
		return nil
	}

	data := WalkData{Root: astRoot, Location: w.loc}
	if err := w.F(&data); err != nil {
		return fmt.Errorf(
			"%s in:\n\n%s",
			err, v.String())
	}

	if data.Replace {
		/*
			if remove {
				w.removeCurrent()
				return nil
			}
		*/

		resultVal := reflect.ValueOf(data.ReplaceValue)
		switch w.loc {
		case reflectwalk.MapKey:
			m := w.cs[len(w.cs)-1]

			var zero reflect.Value
			m.SetMapIndex(w.csData.(reflect.Value), zero)

			m.SetMapIndex(resultVal, w.lastValue)

			w.csData = resultVal
		case reflectwalk.MapValue:

			m := w.cs[len(w.cs)-1]
			mk := w.csData.(reflect.Value)
			m.SetMapIndex(mk, resultVal)
		default:

			setV.Set(resultVal)
		}
	}

	return nil
}

func (w *interpolationWalker) removeCurrent() {

	w.unknownKeys = append(w.unknownKeys, strings.Join(w.key, "."))

	for i := 1; i <= len(w.cs); i++ {
		c := w.cs[len(w.cs)-i]
		switch c.Kind() {
		case reflect.Map:

			var val reflect.Value

			k := w.csData.(reflect.Value)
			c.SetMapIndex(k, val)
			return
		}
	}

	panic("No container found for removeCurrent")
}

func (w *interpolationWalker) replaceCurrent(v reflect.Value) {
	c := w.cs[len(w.cs)-2]
	switch c.Kind() {
	case reflect.Map:

		k := w.csKey[len(w.csKey)-1]
		c.SetMapIndex(k, v)
	}
}

func (w *interpolationWalker) splitSlice() {

	var s []interface{}
	raw := w.cs[len(w.cs)-1]
	switch v := raw.Interface().(type) {
	case []interface{}:
		s = v
	case []map[string]interface{}:
		return
	default:
		panic("Unknown kind: " + raw.Kind().String())
	}

	split := false
	if !split {
		return
	}

	result := make([]interface{}, 0, len(s)*2)

	for _, v := range s {
		sv, ok := v.(string)
		if !ok {

			result = append(result, v)
			continue
		}

		result = append(result, sv)
	}

	w.replaceCurrent(reflect.ValueOf(result))
}
