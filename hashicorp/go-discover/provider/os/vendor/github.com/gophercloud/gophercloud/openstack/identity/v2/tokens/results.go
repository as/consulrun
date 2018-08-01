package tokens

import (
	"time"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack/identity/v2/tenants"
)

type Token struct {
	ID string

	ExpiresAt time.Time

	Tenant tenants.Tenant
}

type Role struct {
	Name string `json:"name"`
}

type User struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	UserName string `json:"username"`
	Roles    []Role `json:"roles"`
}

//
//
//
type Endpoint struct {
	TenantID    string `json:"tenantId"`
	PublicURL   string `json:"publicURL"`
	InternalURL string `json:"internalURL"`
	AdminURL    string `json:"adminURL"`
	Region      string `json:"region"`
	VersionID   string `json:"versionId"`
	VersionInfo string `json:"versionInfo"`
	VersionList string `json:"versionList"`
}

//
//
type CatalogEntry struct {
	Name string `json:"name"`

	Type string `json:"type"`

	Endpoints []Endpoint `json:"endpoints"`
}

type ServiceCatalog struct {
	Entries []CatalogEntry
}

type CreateResult struct {
	gophercloud.Result
}

type GetResult struct {
	CreateResult
}

func (r CreateResult) ExtractToken() (*Token, error) {
	var s struct {
		Access struct {
			Token struct {
				Expires string         `json:"expires"`
				ID      string         `json:"id"`
				Tenant  tenants.Tenant `json:"tenant"`
			} `json:"token"`
		} `json:"access"`
	}

	err := r.ExtractInto(&s)
	if err != nil {
		return nil, err
	}

	expiresTs, err := time.Parse(gophercloud.RFC3339Milli, s.Access.Token.Expires)
	if err != nil {
		return nil, err
	}

	return &Token{
		ID:        s.Access.Token.ID,
		ExpiresAt: expiresTs,
		Tenant:    s.Access.Token.Tenant,
	}, nil
}

func (r CreateResult) ExtractServiceCatalog() (*ServiceCatalog, error) {
	var s struct {
		Access struct {
			Entries []CatalogEntry `json:"serviceCatalog"`
		} `json:"access"`
	}
	err := r.ExtractInto(&s)
	return &ServiceCatalog{Entries: s.Access.Entries}, err
}

func (r GetResult) ExtractUser() (*User, error) {
	var s struct {
		Access struct {
			User User `json:"user"`
		} `json:"access"`
	}
	err := r.ExtractInto(&s)
	return &s.Access.User, err
}
