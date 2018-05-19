package tenants

import (
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/pagination"
)

type ListOpts struct {
	Marker string `q:"marker"`

	Limit int `q:"limit"`
}

func List(client *gophercloud.ServiceClient, opts *ListOpts) pagination.Pager {
	url := listURL(client)
	if opts != nil {
		q, err := gophercloud.BuildQueryString(opts)
		if err != nil {
			return pagination.Pager{Err: err}
		}
		url += q.String()
	}
	return pagination.NewPager(client, url, func(r pagination.PageResult) pagination.Page {
		return TenantPage{pagination.LinkedPageBase{PageResult: r}}
	})
}

type CreateOpts struct {
	Name string `json:"name" required:"true"`

	Description string `json:"description,omitempty"`

	Enabled *bool `json:"enabled,omitempty"`
}

type CreateOptsBuilder interface {
	ToTenantCreateMap() (map[string]interface{}, error)
}

func (opts CreateOpts) ToTenantCreateMap() (map[string]interface{}, error) {
	return gophercloud.BuildRequestBody(opts, "tenant")
}

func Create(client *gophercloud.ServiceClient, opts CreateOptsBuilder) (r CreateResult) {
	b, err := opts.ToTenantCreateMap()
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

type UpdateOptsBuilder interface {
	ToTenantUpdateMap() (map[string]interface{}, error)
}

type UpdateOpts struct {
	Name string `json:"name,omitempty"`

	Description string `json:"description,omitempty"`

	Enabled *bool `json:"enabled,omitempty"`
}

func (opts UpdateOpts) ToTenantUpdateMap() (map[string]interface{}, error) {
	return gophercloud.BuildRequestBody(opts, "tenant")
}

func Update(client *gophercloud.ServiceClient, id string, opts UpdateOptsBuilder) (r UpdateResult) {
	b, err := opts.ToTenantUpdateMap()
	if err != nil {
		r.Err = err
		return
	}
	_, r.Err = client.Put(updateURL(client, id), &b, &r.Body, &gophercloud.RequestOpts{
		OkCodes: []int{200},
	})
	return
}

func Delete(client *gophercloud.ServiceClient, id string) (r DeleteResult) {
	_, r.Err = client.Delete(deleteURL(client, id), nil)
	return
}
