package tenants

import (
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/pagination"
)

type Tenant struct {
	ID string `json:"id"`

	Name string `json:"name"`

	Description string `json:"description"`

	Enabled bool `json:"enabled"`
}

type TenantPage struct {
	pagination.LinkedPageBase
}

func (r TenantPage) IsEmpty() (bool, error) {
	tenants, err := ExtractTenants(r)
	return len(tenants) == 0, err
}

func (r TenantPage) NextPageURL() (string, error) {
	var s struct {
		Links []gophercloud.Link `json:"tenants_links"`
	}
	err := r.ExtractInto(&s)
	if err != nil {
		return "", err
	}
	return gophercloud.ExtractNextURL(s.Links)
}

func ExtractTenants(r pagination.Page) ([]Tenant, error) {
	var s struct {
		Tenants []Tenant `json:"tenants"`
	}
	err := (r.(TenantPage)).ExtractInto(&s)
	return s.Tenants, err
}

type tenantResult struct {
	gophercloud.Result
}

func (r tenantResult) Extract() (*Tenant, error) {
	var s struct {
		Tenant *Tenant `json:"tenant"`
	}
	err := r.ExtractInto(&s)
	return s.Tenant, err
}

type GetResult struct {
	tenantResult
}

type CreateResult struct {
	tenantResult
}

type DeleteResult struct {
	gophercloud.ErrResult
}

type UpdateResult struct {
	tenantResult
}
