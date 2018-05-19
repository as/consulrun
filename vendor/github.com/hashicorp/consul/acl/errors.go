package acl

import (
	"errors"
	"strings"
)

const (
	errNotFound         = "ACL not found"
	errRootDenied       = "Cannot resolve root ACL"
	errDisabled         = "ACL support disabled"
	errPermissionDenied = "Permission denied"
)

var (
	ErrNotFound = errors.New(errNotFound)

	ErrRootDenied = errors.New(errRootDenied)

	ErrDisabled = errors.New(errDisabled)

	ErrPermissionDenied = PermissionDeniedError{}
)

func IsErrNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), errNotFound)
}

func IsErrRootDenied(err error) bool {
	return err != nil && strings.Contains(err.Error(), errRootDenied)
}

func IsErrDisabled(err error) bool {
	return err != nil && strings.Contains(err.Error(), errDisabled)
}

func IsErrPermissionDenied(err error) bool {
	return err != nil && strings.Contains(err.Error(), errPermissionDenied)
}

type PermissionDeniedError struct {
	Cause string
}

func (e PermissionDeniedError) Error() string {
	if e.Cause != "" {
		return errPermissionDenied + ": " + e.Cause
	}
	return errPermissionDenied
}
