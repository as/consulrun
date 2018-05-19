package openstack

import (
	"fmt"

	"github.com/gophercloud/gophercloud"
	tokens2 "github.com/gophercloud/gophercloud/openstack/identity/v2/tokens"
	tokens3 "github.com/gophercloud/gophercloud/openstack/identity/v3/tokens"
)

type ErrEndpointNotFound struct{ gophercloud.BaseError }

func (e ErrEndpointNotFound) Error() string {
	return "No suitable endpoint could be found in the service catalog."
}

type ErrInvalidAvailabilityProvided struct{ gophercloud.ErrInvalidInput }

func (e ErrInvalidAvailabilityProvided) Error() string {
	return fmt.Sprintf("Unexpected availability in endpoint query: %s", e.Value)
}

type ErrMultipleMatchingEndpointsV2 struct {
	gophercloud.BaseError
	Endpoints []tokens2.Endpoint
}

func (e ErrMultipleMatchingEndpointsV2) Error() string {
	return fmt.Sprintf("Discovered %d matching endpoints: %#v", len(e.Endpoints), e.Endpoints)
}

type ErrMultipleMatchingEndpointsV3 struct {
	gophercloud.BaseError
	Endpoints []tokens3.Endpoint
}

func (e ErrMultipleMatchingEndpointsV3) Error() string {
	return fmt.Sprintf("Discovered %d matching endpoints: %#v", len(e.Endpoints), e.Endpoints)
}

type ErrNoAuthURL struct{ gophercloud.ErrInvalidInput }

func (e ErrNoAuthURL) Error() string {
	return "Environment variable OS_AUTH_URL needs to be set."
}

type ErrNoUsername struct{ gophercloud.ErrInvalidInput }

func (e ErrNoUsername) Error() string {
	return "Environment variable OS_USERNAME needs to be set."
}

type ErrNoPassword struct{ gophercloud.ErrInvalidInput }

func (e ErrNoPassword) Error() string {
	return "Environment variable OS_PASSWORD needs to be set."
}
