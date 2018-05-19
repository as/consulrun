package gophercloud

import "fmt"

type BaseError struct {
	DefaultErrString string
	Info             string
}

func (e BaseError) Error() string {
	e.DefaultErrString = "An error occurred while executing a Gophercloud request."
	return e.choseErrString()
}

func (e BaseError) choseErrString() string {
	if e.Info != "" {
		return e.Info
	}
	return e.DefaultErrString
}

type ErrMissingInput struct {
	BaseError
	Argument string
}

func (e ErrMissingInput) Error() string {
	e.DefaultErrString = fmt.Sprintf("Missing input for argument [%s]", e.Argument)
	return e.choseErrString()
}

type ErrInvalidInput struct {
	ErrMissingInput
	Value interface{}
}

func (e ErrInvalidInput) Error() string {
	e.DefaultErrString = fmt.Sprintf("Invalid input provided for argument [%s]: [%+v]", e.Argument, e.Value)
	return e.choseErrString()
}

type ErrUnexpectedResponseCode struct {
	BaseError
	URL      string
	Method   string
	Expected []int
	Actual   int
	Body     []byte
}

func (e ErrUnexpectedResponseCode) Error() string {
	e.DefaultErrString = fmt.Sprintf(
		"Expected HTTP response code %v when accessing [%s %s], but got %d instead\n%s",
		e.Expected, e.Method, e.URL, e.Actual, e.Body,
	)
	return e.choseErrString()
}

type ErrDefault400 struct {
	ErrUnexpectedResponseCode
}

type ErrDefault401 struct {
	ErrUnexpectedResponseCode
}

type ErrDefault404 struct {
	ErrUnexpectedResponseCode
}

type ErrDefault405 struct {
	ErrUnexpectedResponseCode
}

type ErrDefault408 struct {
	ErrUnexpectedResponseCode
}

type ErrDefault429 struct {
	ErrUnexpectedResponseCode
}

type ErrDefault500 struct {
	ErrUnexpectedResponseCode
}

type ErrDefault503 struct {
	ErrUnexpectedResponseCode
}

func (e ErrDefault400) Error() string {
	return "Invalid request due to incorrect syntax or missing required parameters."
}
func (e ErrDefault401) Error() string {
	return "Authentication failed"
}
func (e ErrDefault404) Error() string {
	return "Resource not found"
}
func (e ErrDefault405) Error() string {
	return "Method not allowed"
}
func (e ErrDefault408) Error() string {
	return "The server timed out waiting for the request"
}
func (e ErrDefault429) Error() string {
	return "Too many requests have been sent in a given amount of time. Pause" +
		" requests, wait up to one minute, and try again."
}
func (e ErrDefault500) Error() string {
	return "Internal Server Error"
}
func (e ErrDefault503) Error() string {
	return "The service is currently unable to handle the request due to a temporary" +
		" overloading or maintenance. This is a temporary condition. Try again later."
}

type Err400er interface {
	Error400(ErrUnexpectedResponseCode) error
}

type Err401er interface {
	Error401(ErrUnexpectedResponseCode) error
}

type Err404er interface {
	Error404(ErrUnexpectedResponseCode) error
}

type Err405er interface {
	Error405(ErrUnexpectedResponseCode) error
}

type Err408er interface {
	Error408(ErrUnexpectedResponseCode) error
}

type Err429er interface {
	Error429(ErrUnexpectedResponseCode) error
}

type Err500er interface {
	Error500(ErrUnexpectedResponseCode) error
}

type Err503er interface {
	Error503(ErrUnexpectedResponseCode) error
}

type ErrTimeOut struct {
	BaseError
}

func (e ErrTimeOut) Error() string {
	e.DefaultErrString = "A time out occurred"
	return e.choseErrString()
}

type ErrUnableToReauthenticate struct {
	BaseError
	ErrOriginal error
}

func (e ErrUnableToReauthenticate) Error() string {
	e.DefaultErrString = fmt.Sprintf("Unable to re-authenticate: %s", e.ErrOriginal)
	return e.choseErrString()
}

type ErrErrorAfterReauthentication struct {
	BaseError
	ErrOriginal error
}

func (e ErrErrorAfterReauthentication) Error() string {
	e.DefaultErrString = fmt.Sprintf("Successfully re-authenticated, but got error executing request: %s", e.ErrOriginal)
	return e.choseErrString()
}

type ErrServiceNotFound struct {
	BaseError
}

func (e ErrServiceNotFound) Error() string {
	e.DefaultErrString = "No suitable service could be found in the service catalog."
	return e.choseErrString()
}

type ErrEndpointNotFound struct {
	BaseError
}

func (e ErrEndpointNotFound) Error() string {
	e.DefaultErrString = "No suitable endpoint could be found in the service catalog."
	return e.choseErrString()
}

type ErrResourceNotFound struct {
	BaseError
	Name         string
	ResourceType string
}

func (e ErrResourceNotFound) Error() string {
	e.DefaultErrString = fmt.Sprintf("Unable to find %s with name %s", e.ResourceType, e.Name)
	return e.choseErrString()
}

type ErrMultipleResourcesFound struct {
	BaseError
	Name         string
	Count        int
	ResourceType string
}

func (e ErrMultipleResourcesFound) Error() string {
	e.DefaultErrString = fmt.Sprintf("Found %d %ss matching %s", e.Count, e.ResourceType, e.Name)
	return e.choseErrString()
}

type ErrUnexpectedType struct {
	BaseError
	Expected string
	Actual   string
}

func (e ErrUnexpectedType) Error() string {
	e.DefaultErrString = fmt.Sprintf("Expected %s but got %s", e.Expected, e.Actual)
	return e.choseErrString()
}

func unacceptedAttributeErr(attribute string) string {
	return fmt.Sprintf("The base Identity V3 API does not accept authentication by %s", attribute)
}

func redundantWithTokenErr(attribute string) string {
	return fmt.Sprintf("%s may not be provided when authenticating with a TokenID", attribute)
}

func redundantWithUserID(attribute string) string {
	return fmt.Sprintf("%s may not be provided when authenticating with a UserID", attribute)
}

type ErrAPIKeyProvided struct{ BaseError }

func (e ErrAPIKeyProvided) Error() string {
	return unacceptedAttributeErr("APIKey")
}

type ErrTenantIDProvided struct{ BaseError }

func (e ErrTenantIDProvided) Error() string {
	return unacceptedAttributeErr("TenantID")
}

type ErrTenantNameProvided struct{ BaseError }

func (e ErrTenantNameProvided) Error() string {
	return unacceptedAttributeErr("TenantName")
}

type ErrUsernameWithToken struct{ BaseError }

func (e ErrUsernameWithToken) Error() string {
	return redundantWithTokenErr("Username")
}

type ErrUserIDWithToken struct{ BaseError }

func (e ErrUserIDWithToken) Error() string {
	return redundantWithTokenErr("UserID")
}

type ErrDomainIDWithToken struct{ BaseError }

func (e ErrDomainIDWithToken) Error() string {
	return redundantWithTokenErr("DomainID")
}

type ErrDomainNameWithToken struct{ BaseError }

func (e ErrDomainNameWithToken) Error() string {
	return redundantWithTokenErr("DomainName")
}

type ErrUsernameOrUserID struct{ BaseError }

func (e ErrUsernameOrUserID) Error() string {
	return "Exactly one of Username and UserID must be provided for password authentication"
}

type ErrDomainIDWithUserID struct{ BaseError }

func (e ErrDomainIDWithUserID) Error() string {
	return redundantWithUserID("DomainID")
}

type ErrDomainNameWithUserID struct{ BaseError }

func (e ErrDomainNameWithUserID) Error() string {
	return redundantWithUserID("DomainName")
}

type ErrDomainIDOrDomainName struct{ BaseError }

func (e ErrDomainIDOrDomainName) Error() string {
	return "You must provide exactly one of DomainID or DomainName to authenticate by Username"
}

type ErrMissingPassword struct{ BaseError }

func (e ErrMissingPassword) Error() string {
	return "You must provide a password to authenticate"
}

type ErrScopeDomainIDOrDomainName struct{ BaseError }

func (e ErrScopeDomainIDOrDomainName) Error() string {
	return "You must provide exactly one of DomainID or DomainName in a Scope with ProjectName"
}

type ErrScopeProjectIDOrProjectName struct{ BaseError }

func (e ErrScopeProjectIDOrProjectName) Error() string {
	return "You must provide at most one of ProjectID or ProjectName in a Scope"
}

type ErrScopeProjectIDAlone struct{ BaseError }

func (e ErrScopeProjectIDAlone) Error() string {
	return "ProjectID must be supplied alone in a Scope"
}

type ErrScopeEmpty struct{ BaseError }

func (e ErrScopeEmpty) Error() string {
	return "You must provide either a Project or Domain in a Scope"
}
