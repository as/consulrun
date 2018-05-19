package multierror

//
func Append(err error, errs ...error) *Error {
	switch err := err.(type) {
	case *Error:

		if err == nil {
			err = new(Error)
		}

		for _, e := range errs {
			switch e := e.(type) {
			case *Error:
				err.Errors = append(err.Errors, e.Errors...)
			default:
				err.Errors = append(err.Errors, e)
			}
		}

		return err
	default:
		newErrs := make([]error, 0, len(errs)+1)
		if err != nil {
			newErrs = append(newErrs, err)
		}
		newErrs = append(newErrs, errs...)

		return Append(&Error{}, newErrs...)
	}
}
