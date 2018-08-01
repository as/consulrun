package multierror

import (
	"fmt"

	"github.com/as/consulrun/hashicorp/errwrap"
)

//
func Prefix(err error, prefix string) error {
	if err == nil {
		return nil
	}

	format := fmt.Sprintf("%s {{err}}", prefix)
	switch err := err.(type) {
	case *Error:

		if err == nil {
			err = new(Error)
		}

		for i, e := range err.Errors {
			err.Errors[i] = errwrap.Wrapf(format, e)
		}

		return err
	default:
		return errwrap.Wrapf(format, err)
	}
}
