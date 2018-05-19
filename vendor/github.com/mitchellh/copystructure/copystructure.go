package copystructure

import (
	"reflect"

	"github.com/mitchellh/reflectwalk"
)

func Copy(v interface{}) (interface{}, error) {
	w := new(walker)
	err := reflectwalk.Walk(v, w)
	if err != nil {
		return nil, err
	}

	result := w.Result
	if result == nil {
		val := reflect.ValueOf(v)
		result = reflect.Indirect(reflect.New(val.Type())).Interface()
	}

	return result, nil
}

type CopierFunc func(interface{}) (interface{}, error)

//
//
var Copiers map[reflect.Type]CopierFunc = make(map[reflect.Type]CopierFunc)

type walker struct {
	Result interface{}

	depth       int
	ignoreDepth int
	vals        []reflect.Value
	cs          []reflect.Value
	ps          []bool
}

func (w *walker) Enter(l reflectwalk.Location) error {
	w.depth++
	return nil
}

func (w *walker) Exit(l reflectwalk.Location) error {
	w.depth--
	if w.ignoreDepth > w.depth {
		w.ignoreDepth = 0
	}

	if w.ignoring() {
		return nil
	}

	switch l {
	case reflectwalk.Map:
		fallthrough
	case reflectwalk.Slice:

		w.cs = w.cs[:len(w.cs)-1]
	case reflectwalk.MapValue:

		mv := w.valPop()
		mk := w.valPop()
		m := w.cs[len(w.cs)-1]
		m.SetMapIndex(mk, mv)
	case reflectwalk.SliceElem:

		v := w.valPop()
		i := w.valPop().Interface().(int)
		s := w.cs[len(w.cs)-1]
		s.Index(i).Set(v)
	case reflectwalk.Struct:
		w.replacePointerMaybe()

		w.cs = w.cs[:len(w.cs)-1]
	case reflectwalk.StructField:

		v := w.valPop()
		f := w.valPop().Interface().(reflect.StructField)
		if v.IsValid() {
			s := w.cs[len(w.cs)-1]
			sf := reflect.Indirect(s).FieldByName(f.Name)
			if sf.CanSet() {
				sf.Set(v)
			}
		}
	case reflectwalk.WalkLoc:

		w.cs = nil
		w.vals = nil
	}

	return nil
}

func (w *walker) Map(m reflect.Value) error {
	if w.ignoring() {
		return nil
	}

	var newMap reflect.Value
	if m.IsNil() {
		newMap = reflect.Indirect(reflect.New(m.Type()))
	} else {
		newMap = reflect.MakeMap(m.Type())
	}

	w.cs = append(w.cs, newMap)
	w.valPush(newMap)
	return nil
}

func (w *walker) MapElem(m, k, v reflect.Value) error {
	return nil
}

func (w *walker) PointerEnter(v bool) error {
	if w.ignoring() {
		return nil
	}

	w.ps = append(w.ps, v)
	return nil
}

func (w *walker) PointerExit(bool) error {
	if w.ignoring() {
		return nil
	}

	w.ps = w.ps[:len(w.ps)-1]
	return nil
}

func (w *walker) Primitive(v reflect.Value) error {
	if w.ignoring() {
		return nil
	}

	var newV reflect.Value
	if v.IsValid() && v.CanInterface() {
		newV = reflect.New(v.Type())
		reflect.Indirect(newV).Set(v)
	}

	w.valPush(newV)
	w.replacePointerMaybe()
	return nil
}

func (w *walker) Slice(s reflect.Value) error {
	if w.ignoring() {
		return nil
	}

	var newS reflect.Value
	if s.IsNil() {
		newS = reflect.Indirect(reflect.New(s.Type()))
	} else {
		newS = reflect.MakeSlice(s.Type(), s.Len(), s.Cap())
	}

	w.cs = append(w.cs, newS)
	w.valPush(newS)
	return nil
}

func (w *walker) SliceElem(i int, elem reflect.Value) error {
	if w.ignoring() {
		return nil
	}

	w.valPush(reflect.ValueOf(i))

	return nil
}

func (w *walker) Struct(s reflect.Value) error {
	if w.ignoring() {
		return nil
	}

	var v reflect.Value
	if c, ok := Copiers[s.Type()]; ok {

		w.ignoreDepth = w.depth

		dup, err := c(s.Interface())
		if err != nil {
			return err
		}

		v = reflect.ValueOf(dup)
	} else {

		v = reflect.New(s.Type())
	}

	w.valPush(v)
	w.cs = append(w.cs, v)

	return nil
}

func (w *walker) StructField(f reflect.StructField, v reflect.Value) error {
	if w.ignoring() {
		return nil
	}

	w.valPush(reflect.ValueOf(f))
	return nil
}

func (w *walker) ignoring() bool {
	return w.ignoreDepth > 0 && w.depth >= w.ignoreDepth
}

func (w *walker) pointerPeek() bool {
	return w.ps[len(w.ps)-1]
}

func (w *walker) valPop() reflect.Value {
	result := w.vals[len(w.vals)-1]
	w.vals = w.vals[:len(w.vals)-1]

	if len(w.vals) == 0 {
		w.Result = nil
	}

	return result
}

func (w *walker) valPush(v reflect.Value) {
	w.vals = append(w.vals, v)

	if w.Result == nil && v.IsValid() {
		w.Result = v.Interface()
	}
}

func (w *walker) replacePointerMaybe() {

	if !w.pointerPeek() {
		w.valPush(reflect.Indirect(w.valPop()))
	}
}
