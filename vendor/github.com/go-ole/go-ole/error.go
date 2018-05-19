package ole

type OleError struct {
	hr          uintptr
	description string
	subError    error
}

func NewError(hr uintptr) *OleError {
	return &OleError{hr: hr}
}

func NewErrorWithDescription(hr uintptr, description string) *OleError {
	return &OleError{hr: hr, description: description}
}

func NewErrorWithSubError(hr uintptr, description string, err error) *OleError {
	return &OleError{hr: hr, description: description, subError: err}
}

func (v *OleError) Code() uintptr {
	return uintptr(v.hr)
}

func (v *OleError) String() string {
	if v.description != "" {
		return errstr(int(v.hr)) + " (" + v.description + ")"
	}
	return errstr(int(v.hr))
}

func (v *OleError) Error() string {
	return v.String()
}

func (v *OleError) Description() string {
	return v.description
}

func (v *OleError) SubError() error {
	return v.subError
}
