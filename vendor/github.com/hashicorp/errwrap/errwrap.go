// Package errwrap implements methods to formalize error wrapping in Go.
//
package errwrap

import (
	"errors"
	"reflect"
	"strings"
)

type WalkFunc func(error)

//
type Wrapper interface {
	WrappedErrors() []error
}

//
func Wrap(outer, inner error) error {
	return &wrappedError{
		Outer: outer,
		Inner: inner,
	}
}

//
func Wrapf(format string, err error) error {
	outerMsg := "<nil>"
	if err != nil {
		outerMsg = err.Error()
	}

	outer := errors.New(strings.Replace(
		format, "{{err}}", outerMsg, -1))

	return Wrap(outer, err)
}

func Contains(err error, msg string) bool {
	return len(GetAll(err, msg)) > 0
}

func ContainsType(err error, v interface{}) bool {
	return len(GetAllType(err, v)) > 0
}

func Get(err error, msg string) error {
	es := GetAll(err, msg)
	if len(es) > 0 {
		return es[len(es)-1]
	}

	return nil
}

func GetType(err error, v interface{}) error {
	es := GetAllType(err, v)
	if len(es) > 0 {
		return es[len(es)-1]
	}

	return nil
}

func GetAll(err error, msg string) []error {
	var result []error

	Walk(err, func(err error) {
		if err.Error() == msg {
			result = append(result, err)
		}
	})

	return result
}

//
func GetAllType(err error, v interface{}) []error {
	var result []error

	var search string
	if v != nil {
		search = reflect.TypeOf(v).String()
	}
	Walk(err, func(err error) {
		var needle string
		if err != nil {
			needle = reflect.TypeOf(err).String()
		}

		if needle == search {
			result = append(result, err)
		}
	})

	return result
}

func Walk(err error, cb WalkFunc) {
	if err == nil {
		return
	}

	switch e := err.(type) {
	case *wrappedError:
		cb(e.Outer)
		Walk(e.Inner, cb)
	case Wrapper:
		cb(err)

		for _, err := range e.WrappedErrors() {
			Walk(err, cb)
		}
	default:
		cb(err)
	}
}

type wrappedError struct {
	Outer error
	Inner error
}

func (w *wrappedError) Error() string {
	return w.Outer.Error()
}

func (w *wrappedError) WrappedErrors() []error {
	return []error{w.Outer, w.Inner}
}
