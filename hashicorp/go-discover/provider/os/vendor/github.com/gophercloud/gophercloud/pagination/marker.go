package pagination

import (
	"fmt"
	"reflect"

	"github.com/gophercloud/gophercloud"
)

type MarkerPage interface {
	Page

	LastMarker() (string, error)
}

type MarkerPageBase struct {
	PageResult

	Owner MarkerPage
}

func (current MarkerPageBase) NextPageURL() (string, error) {
	currentURL := current.URL

	mark, err := current.Owner.LastMarker()
	if err != nil {
		return "", err
	}

	q := currentURL.Query()
	q.Set("marker", mark)
	currentURL.RawQuery = q.Encode()

	return currentURL.String(), nil
}

func (current MarkerPageBase) IsEmpty() (bool, error) {
	if b, ok := current.Body.([]interface{}); ok {
		return len(b) == 0, nil
	}
	err := gophercloud.ErrUnexpectedType{}
	err.Expected = "[]interface{}"
	err.Actual = fmt.Sprintf("%v", reflect.TypeOf(current.Body))
	return true, err
}

func (current MarkerPageBase) GetBody() interface{} {
	return current.Body
}
