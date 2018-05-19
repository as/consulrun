// Copyright (c) 2012, 2013 Ugorji Nwoke. All rights reserved.

package codec

import (
	"errors"
	"fmt"
	"math"
	"reflect"
)

var (
	raisePanicAfterRecover = false
	debugging              = true
)

func panicValToErr(panicVal interface{}, err *error) {
	switch xerr := panicVal.(type) {
	case error:
		*err = xerr
	case string:
		*err = errors.New(xerr)
	default:
		*err = fmt.Errorf("%v", panicVal)
	}
	if raisePanicAfterRecover {
		panic(panicVal)
	}
	return
}

func isEmptyValueDeref(v reflect.Value, deref bool) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		if deref {
			if v.IsNil() {
				return true
			}
			return isEmptyValueDeref(v.Elem(), deref)
		} else {
			return v.IsNil()
		}
	case reflect.Struct:

		for i, n := 0, v.NumField(); i < n; i++ {
			if !isEmptyValueDeref(v.Field(i), deref) {
				return false
			}
		}
		return true
	}
	return false
}

func isEmptyValue(v reflect.Value) bool {
	return isEmptyValueDeref(v, true)
}

func debugf(format string, args ...interface{}) {
	if debugging {
		if len(format) == 0 || format[len(format)-1] != '\n' {
			format = format + "\n"
		}
		fmt.Printf(format, args...)
	}
}

func pruneSignExt(v []byte, pos bool) (n int) {
	if len(v) < 2 {
	} else if pos && v[0] == 0 {
		for ; v[n] == 0 && n+1 < len(v) && (v[n+1]&(1<<7) == 0); n++ {
		}
	} else if !pos && v[0] == 0xff {
		for ; v[n] == 0xff && n+1 < len(v) && (v[n+1]&(1<<7) != 0); n++ {
		}
	}
	return
}

func implementsIntf(typ, iTyp reflect.Type) (success bool, indir int8) {
	if typ == nil {
		return
	}
	rt := typ

	for {
		if rt.Implements(iTyp) {
			return true, indir
		}
		if p := rt; p.Kind() == reflect.Ptr {
			indir++
			if indir >= math.MaxInt8 { // insane number of indirections
				return false, 0
			}
			rt = p.Elem()
			continue
		}
		break
	}

	if typ.Kind() != reflect.Ptr {

		if reflect.PtrTo(typ).Implements(iTyp) {
			return true, -1
		}
	}
	return false, 0
}
