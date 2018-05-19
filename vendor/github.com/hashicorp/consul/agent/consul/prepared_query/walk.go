package prepared_query

import (
	"fmt"
	"reflect"
)

type visitor func(path string, v reflect.Value) error

func visit(path string, v reflect.Value, t reflect.Type, fn visitor) error {
	switch v.Kind() {
	case reflect.String:
		return fn(path, v)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			vf := v.Field(i)
			tf := t.Field(i)
			newPath := fmt.Sprintf("%s.%s", path, tf.Name)
			if err := visit(newPath, vf, tf.Type, fn); err != nil {
				return err
			}
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			vi := v.Index(i)
			ti := vi.Type()
			newPath := fmt.Sprintf("%s[%d]", path, i)
			if err := visit(newPath, vi, ti, fn); err != nil {
				return err
			}
		}
	case reflect.Map:
		for _, key := range v.MapKeys() {
			value := v.MapIndex(key)

			newValue := reflect.New(value.Type()).Elem()
			newValue.SetString(value.String())

			if err := visit(fmt.Sprintf("%s[%s]", path, key.String()), newValue, newValue.Type(), fn); err != nil {
				return err
			}

			v.SetMapIndex(key, newValue)
		}
	}
	return nil
}

func walk(obj interface{}, fn visitor) error {
	v := reflect.ValueOf(obj).Elem()
	t := v.Type()
	return visit("", v, t, fn)
}
