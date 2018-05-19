package config

import "strings"

//
//
//
func TranslateKeys(v map[string]interface{}, dict map[string]string) {
	ck(v, dict)
}

func ck(v interface{}, dict map[string]string) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		for k, v := range x {
			canonKey := dict[strings.ToLower(k)]

			if canonKey == "" {
				x[k] = ck(v, dict)
				continue
			}

			delete(x, k)

			if _, ok := x[canonKey]; ok {
				continue
			}

			x[canonKey] = ck(v, dict)
		}
		return x

	case []interface{}:
		var a []interface{}
		for _, xv := range x {
			a = append(a, ck(xv, dict))
		}
		return a

	default:
		return v
	}
}
