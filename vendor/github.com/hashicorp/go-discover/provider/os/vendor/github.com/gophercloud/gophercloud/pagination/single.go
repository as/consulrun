package pagination

import (
	"fmt"
	"reflect"

	"github.com/gophercloud/gophercloud"
)

type SinglePageBase PageResult

func (current SinglePageBase) NextPageURL() (string, error) {
	return "", nil
}

func (current SinglePageBase) IsEmpty() (bool, error) {
	if b, ok := current.Body.([]interface{}); ok {
		return len(b) == 0, nil
	}
	err := gophercloud.ErrUnexpectedType{}
	err.Expected = "[]interface{}"
	err.Actual = fmt.Sprintf("%v", reflect.TypeOf(current.Body))
	return true, err
}

func (current SinglePageBase) GetBody() interface{} {
	return current.Body
}
