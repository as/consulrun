package multierror

import (
	"fmt"
)

type Error struct {
	Errors      []error
	ErrorFormat ErrorFormatFunc
}

func (e *Error) Error() string {
	fn := e.ErrorFormat
	if fn == nil {
		fn = ListFormatFunc
	}

	return fn(e.Errors)
}

func (e *Error) ErrorOrNil() error {
	if e == nil {
		return nil
	}
	if len(e.Errors) == 0 {
		return nil
	}

	return e
}

func (e *Error) GoString() string {
	return fmt.Sprintf("*%#v", *e)
}

//
func (e *Error) WrappedErrors() []error {
	return e.Errors
}
