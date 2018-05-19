package pagination

import (
	"fmt"
	"reflect"

	"github.com/gophercloud/gophercloud"
)

type LinkedPageBase struct {
	PageResult

	LinkPath []string
}

func (current LinkedPageBase) NextPageURL() (string, error) {
	var path []string
	var key string

	if current.LinkPath == nil {
		path = []string{"links", "next"}
	} else {
		path = current.LinkPath
	}

	submap, ok := current.Body.(map[string]interface{})
	if !ok {
		err := gophercloud.ErrUnexpectedType{}
		err.Expected = "map[string]interface{}"
		err.Actual = fmt.Sprintf("%v", reflect.TypeOf(current.Body))
		return "", err
	}

	for {
		key, path = path[0], path[1:]

		value, ok := submap[key]
		if !ok {
			return "", nil
		}

		if len(path) > 0 {
			submap, ok = value.(map[string]interface{})
			if !ok {
				err := gophercloud.ErrUnexpectedType{}
				err.Expected = "map[string]interface{}"
				err.Actual = fmt.Sprintf("%v", reflect.TypeOf(value))
				return "", err
			}
		} else {
			if value == nil {

				return "", nil
			}

			url, ok := value.(string)
			if !ok {
				err := gophercloud.ErrUnexpectedType{}
				err.Expected = "string"
				err.Actual = fmt.Sprintf("%v", reflect.TypeOf(value))
				return "", err
			}

			return url, nil
		}
	}
}

func (current LinkedPageBase) IsEmpty() (bool, error) {
	if b, ok := current.Body.([]interface{}); ok {
		return len(b) == 0, nil
	}
	err := gophercloud.ErrUnexpectedType{}
	err.Expected = "[]interface{}"
	err.Actual = fmt.Sprintf("%v", reflect.TypeOf(current.Body))
	return true, err
}

func (current LinkedPageBase) GetBody() interface{} {
	return current.Body
}
