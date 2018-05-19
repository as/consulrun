package hashstructure

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"reflect"
)

type ErrNotStringer struct {
	Field string
}

func (ens *ErrNotStringer) Error() string {
	return fmt.Sprintf("hashstructure: %s has hash:\"string\" set, but does not implement fmt.Stringer", ens.Field)
}

type HashOptions struct {
	Hasher hash.Hash64

	TagName string

	ZeroNil bool
}

//
//
//
//
//
//
//
//
//
//
//
func Hash(v interface{}, opts *HashOptions) (uint64, error) {

	if opts == nil {
		opts = &HashOptions{}
	}
	if opts.Hasher == nil {
		opts.Hasher = fnv.New64()
	}
	if opts.TagName == "" {
		opts.TagName = "hash"
	}

	opts.Hasher.Reset()

	w := &walker{
		h:       opts.Hasher,
		tag:     opts.TagName,
		zeronil: opts.ZeroNil,
	}
	return w.visit(reflect.ValueOf(v), nil)
}

type walker struct {
	h       hash.Hash64
	tag     string
	zeronil bool
}

type visitOpts struct {
	Flags visitFlag

	Struct      interface{}
	StructField string
}

func (w *walker) visit(v reflect.Value, opts *visitOpts) (uint64, error) {
	t := reflect.TypeOf(0)

	for {

		if v.Kind() == reflect.Interface {
			v = v.Elem()
			continue
		}

		if v.Kind() == reflect.Ptr {
			if w.zeronil {
				t = v.Type().Elem()
			}
			v = reflect.Indirect(v)
			continue
		}

		break
	}

	if !v.IsValid() {
		v = reflect.Zero(t)
	}

	switch v.Kind() {
	case reflect.Int:
		v = reflect.ValueOf(int64(v.Int()))
	case reflect.Uint:
		v = reflect.ValueOf(uint64(v.Uint()))
	case reflect.Bool:
		var tmp int8
		if v.Bool() {
			tmp = 1
		}
		v = reflect.ValueOf(tmp)
	}

	k := v.Kind()

	if k >= reflect.Int && k <= reflect.Complex64 {

		w.h.Reset()
		err := binary.Write(w.h, binary.LittleEndian, v.Interface())
		return w.h.Sum64(), err
	}

	switch k {
	case reflect.Array:
		var h uint64
		l := v.Len()
		for i := 0; i < l; i++ {
			current, err := w.visit(v.Index(i), nil)
			if err != nil {
				return 0, err
			}

			h = hashUpdateOrdered(w.h, h, current)
		}

		return h, nil

	case reflect.Map:
		var includeMap IncludableMap
		if opts != nil && opts.Struct != nil {
			if v, ok := opts.Struct.(IncludableMap); ok {
				includeMap = v
			}
		}

		var h uint64
		for _, k := range v.MapKeys() {
			v := v.MapIndex(k)
			if includeMap != nil {
				incl, err := includeMap.HashIncludeMap(
					opts.StructField, k.Interface(), v.Interface())
				if err != nil {
					return 0, err
				}
				if !incl {
					continue
				}
			}

			kh, err := w.visit(k, nil)
			if err != nil {
				return 0, err
			}
			vh, err := w.visit(v, nil)
			if err != nil {
				return 0, err
			}

			fieldHash := hashUpdateOrdered(w.h, kh, vh)
			h = hashUpdateUnordered(h, fieldHash)
		}

		return h, nil

	case reflect.Struct:
		parent := v.Interface()
		var include Includable
		if impl, ok := parent.(Includable); ok {
			include = impl
		}

		t := v.Type()
		h, err := w.visit(reflect.ValueOf(t.Name()), nil)
		if err != nil {
			return 0, err
		}

		l := v.NumField()
		for i := 0; i < l; i++ {
			if innerV := v.Field(i); v.CanSet() || t.Field(i).Name != "_" {
				var f visitFlag
				fieldType := t.Field(i)
				if fieldType.PkgPath != "" {

					continue
				}

				tag := fieldType.Tag.Get(w.tag)
				if tag == "ignore" || tag == "-" {

					continue
				}

				if tag == "string" {
					if impl, ok := innerV.Interface().(fmt.Stringer); ok {
						innerV = reflect.ValueOf(impl.String())
					} else {
						return 0, &ErrNotStringer{
							Field: v.Type().Field(i).Name,
						}
					}
				}

				if include != nil {
					incl, err := include.HashInclude(fieldType.Name, innerV)
					if err != nil {
						return 0, err
					}
					if !incl {
						continue
					}
				}

				switch tag {
				case "set":
					f |= visitFlagSet
				}

				kh, err := w.visit(reflect.ValueOf(fieldType.Name), nil)
				if err != nil {
					return 0, err
				}

				vh, err := w.visit(innerV, &visitOpts{
					Flags:       f,
					Struct:      parent,
					StructField: fieldType.Name,
				})
				if err != nil {
					return 0, err
				}

				fieldHash := hashUpdateOrdered(w.h, kh, vh)
				h = hashUpdateUnordered(h, fieldHash)
			}
		}

		return h, nil

	case reflect.Slice:

		var h uint64
		var set bool
		if opts != nil {
			set = (opts.Flags & visitFlagSet) != 0
		}
		l := v.Len()
		for i := 0; i < l; i++ {
			current, err := w.visit(v.Index(i), nil)
			if err != nil {
				return 0, err
			}

			if set {
				h = hashUpdateUnordered(h, current)
			} else {
				h = hashUpdateOrdered(w.h, h, current)
			}
		}

		return h, nil

	case reflect.String:

		w.h.Reset()
		_, err := w.h.Write([]byte(v.String()))
		return w.h.Sum64(), err

	default:
		return 0, fmt.Errorf("unknown kind to hash: %s", k)
	}

}

func hashUpdateOrdered(h hash.Hash64, a, b uint64) uint64 {

	h.Reset()

	e1 := binary.Write(h, binary.LittleEndian, a)
	e2 := binary.Write(h, binary.LittleEndian, b)
	if e1 != nil {
		panic(e1)
	}
	if e2 != nil {
		panic(e2)
	}

	return h.Sum64()
}

func hashUpdateUnordered(a, b uint64) uint64 {
	return a ^ b
}

type visitFlag uint

const (
	visitFlagInvalid visitFlag = iota
	visitFlagSet               = iota << 1
)
