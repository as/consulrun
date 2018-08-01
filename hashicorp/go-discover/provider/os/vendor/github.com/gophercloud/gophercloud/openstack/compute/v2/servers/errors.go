package servers

import (
	"fmt"

	"github.com/gophercloud/gophercloud"
)

type ErrNeitherImageIDNorImageNameProvided struct{ gophercloud.ErrMissingInput }

func (e ErrNeitherImageIDNorImageNameProvided) Error() string {
	return "One and only one of the image ID and the image name must be provided."
}

type ErrNeitherFlavorIDNorFlavorNameProvided struct{ gophercloud.ErrMissingInput }

func (e ErrNeitherFlavorIDNorFlavorNameProvided) Error() string {
	return "One and only one of the flavor ID and the flavor name must be provided."
}

type ErrNoClientProvidedForIDByName struct{ gophercloud.ErrMissingInput }

func (e ErrNoClientProvidedForIDByName) Error() string {
	return "A service client must be provided to find a resource ID by name."
}

type ErrInvalidHowParameterProvided struct{ gophercloud.ErrInvalidInput }

type ErrNoAdminPassProvided struct{ gophercloud.ErrMissingInput }

type ErrNoImageIDProvided struct{ gophercloud.ErrMissingInput }

type ErrNoIDProvided struct{ gophercloud.ErrMissingInput }

type ErrServer struct {
	gophercloud.ErrUnexpectedResponseCode
	ID string
}

func (se ErrServer) Error() string {
	return fmt.Sprintf("Error while executing HTTP request for server [%s]", se.ID)
}

func (se ErrServer) Error404(e gophercloud.ErrUnexpectedResponseCode) error {
	se.ErrUnexpectedResponseCode = e
	return &ErrServerNotFound{se}
}

type ErrServerNotFound struct {
	ErrServer
}

func (e ErrServerNotFound) Error() string {
	return fmt.Sprintf("I couldn't find server [%s]", e.ID)
}
