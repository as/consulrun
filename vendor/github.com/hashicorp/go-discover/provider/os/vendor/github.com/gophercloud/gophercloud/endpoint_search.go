package gophercloud

type Availability string

const (
	AvailabilityAdmin Availability = "admin"

	AvailabilityPublic Availability = "public"

	AvailabilityInternal Availability = "internal"
)

//
// package, like "openstack.NewComputeV2()".
type EndpointOpts struct {
	Type string

	Name string

	Region string

	//

	Availability Availability
}

/*
EndpointLocator is an internal function to be used by provider implementations.

It provides an implementation that locates a single endpoint from a service
catalog for a specific ProviderClient based on user-provided EndpointOpts. The
provider then uses it to discover related ServiceClients.
*/
type EndpointLocator func(EndpointOpts) (string, error)

//
func (eo *EndpointOpts) ApplyDefaults(t string) {
	if eo.Type == "" {
		eo.Type = t
	}
	if eo.Availability == "" {
		eo.Availability = AvailabilityPublic
	}
}
