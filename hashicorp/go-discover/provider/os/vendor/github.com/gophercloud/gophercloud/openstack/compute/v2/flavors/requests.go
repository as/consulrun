package flavors

import (
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/pagination"
)

type ListOptsBuilder interface {
	ToFlavorListQuery() (string, error)
}

/*
	AccessType maps to OpenStack's Flavor.is_public field. Although the is_public
	field is boolean, the request options are ternary, which is why AccessType is
	a string. The following values are allowed:

	The AccessType arguement is optional, and if it is not supplied, OpenStack
	returns the PublicAccess flavors.
*/
type AccessType string

const (
	PublicAccess AccessType = "true"

	PrivateAccess AccessType = "false"

	AllAccess AccessType = "None"
)

/*
	ListOpts filters the results returned by the List() function.
	For example, a flavor with a minDisk field of 10 will not be returned if you
	specify MinDisk set to 20.

	Typically, software will use the last ID of the previous call to List to set
	the Marker for the current call.
*/
type ListOpts struct {
	ChangesSince string `q:"changes-since"`

	MinDisk int `q:"minDisk"`
	MinRAM  int `q:"minRam"`

	Marker string `q:"marker"`

	Limit int `q:"limit"`

	AccessType AccessType `q:"is_public"`
}

func (opts ListOpts) ToFlavorListQuery() (string, error) {
	q, err := gophercloud.BuildQueryString(opts)
	return q.String(), err
}

func ListDetail(client *gophercloud.ServiceClient, opts ListOptsBuilder) pagination.Pager {
	url := listURL(client)
	if opts != nil {
		query, err := opts.ToFlavorListQuery()
		if err != nil {
			return pagination.Pager{Err: err}
		}
		url += query
	}
	return pagination.NewPager(client, url, func(r pagination.PageResult) pagination.Page {
		return FlavorPage{pagination.LinkedPageBase{PageResult: r}}
	})
}

type CreateOptsBuilder interface {
	ToFlavorCreateMap() (map[string]interface{}, error)
}

type CreateOpts struct {
	Name string `json:"name" required:"true"`

	RAM int `json:"ram" required:"true"`

	VCPUs int `json:"vcpus" required:"true"`

	Disk *int `json:"disk" required:"true"`

	ID string `json:"id,omitempty"`

	Swap *int `json:"swap,omitempty"`

	RxTxFactor float64 `json:"rxtx_factor,omitempty"`

	IsPublic *bool `json:"os-flavor-access:is_public,omitempty"`

	Ephemeral *int `json:"OS-FLV-EXT-DATA:ephemeral,omitempty"`
}

func (opts CreateOpts) ToFlavorCreateMap() (map[string]interface{}, error) {
	return gophercloud.BuildRequestBody(opts, "flavor")
}

func Create(client *gophercloud.ServiceClient, opts CreateOptsBuilder) (r CreateResult) {
	b, err := opts.ToFlavorCreateMap()
	if err != nil {
		r.Err = err
		return
	}
	_, r.Err = client.Post(createURL(client), b, &r.Body, &gophercloud.RequestOpts{
		OkCodes: []int{200, 201},
	})
	return
}

func Get(client *gophercloud.ServiceClient, id string) (r GetResult) {
	_, r.Err = client.Get(getURL(client, id), &r.Body, nil)
	return
}

func Delete(client *gophercloud.ServiceClient, id string) (r DeleteResult) {
	_, r.Err = client.Delete(deleteURL(client, id), nil)
	return
}

func IDFromName(client *gophercloud.ServiceClient, name string) (string, error) {
	count := 0
	id := ""
	allPages, err := ListDetail(client, nil).AllPages()
	if err != nil {
		return "", err
	}

	all, err := ExtractFlavors(allPages)
	if err != nil {
		return "", err
	}

	for _, f := range all {
		if f.Name == name {
			count++
			id = f.ID
		}
	}

	switch count {
	case 0:
		err := &gophercloud.ErrResourceNotFound{}
		err.ResourceType = "flavor"
		err.Name = name
		return "", err
	case 1:
		return id, nil
	default:
		err := &gophercloud.ErrMultipleResourcesFound{}
		err.ResourceType = "flavor"
		err.Name = name
		err.Count = count
		return "", err
	}
}
