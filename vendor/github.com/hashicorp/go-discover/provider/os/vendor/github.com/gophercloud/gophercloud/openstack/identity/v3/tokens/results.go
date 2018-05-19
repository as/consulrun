package tokens

import (
	"time"

	"github.com/gophercloud/gophercloud"
)

type Endpoint struct {
	ID        string `json:"id"`
	Region    string `json:"region"`
	Interface string `json:"interface"`
	URL       string `json:"url"`
}

//
type CatalogEntry struct {
	ID string `json:"id"`

	Name string `json:"name"`

	Type string `json:"type"`

	Endpoints []Endpoint `json:"endpoints"`
}

type ServiceCatalog struct {
	Entries []CatalogEntry `json:"catalog"`
}

type Domain struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type User struct {
	Domain Domain `json:"domain"`
	ID     string `json:"id"`
	Name   string `json:"name"`
}

type Role struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Project struct {
	Domain Domain `json:"domain"`
	ID     string `json:"id"`
	Name   string `json:"name"`
}

type commonResult struct {
	gophercloud.Result
}

func (r commonResult) Extract() (*Token, error) {
	return r.ExtractToken()
}

func (r commonResult) ExtractToken() (*Token, error) {
	var s Token
	err := r.ExtractInto(&s)
	if err != nil {
		return nil, err
	}

	s.ID = r.Header.Get("X-Subject-Token")

	return &s, err
}

func (r commonResult) ExtractServiceCatalog() (*ServiceCatalog, error) {
	var s ServiceCatalog
	err := r.ExtractInto(&s)
	return &s, err
}

func (r commonResult) ExtractUser() (*User, error) {
	var s struct {
		User *User `json:"user"`
	}
	err := r.ExtractInto(&s)
	return s.User, err
}

func (r commonResult) ExtractRoles() ([]Role, error) {
	var s struct {
		Roles []Role `json:"roles"`
	}
	err := r.ExtractInto(&s)
	return s.Roles, err
}

func (r commonResult) ExtractProject() (*Project, error) {
	var s struct {
		Project *Project `json:"project"`
	}
	err := r.ExtractInto(&s)
	return s.Project, err
}

type CreateResult struct {
	commonResult
}

type GetResult struct {
	commonResult
}

type RevokeResult struct {
	commonResult
}

type Token struct {
	ID string `json:"id"`

	ExpiresAt time.Time `json:"expires_at"`
}

func (r commonResult) ExtractInto(v interface{}) error {
	return r.ExtractIntoStructPtr(v, "token")
}
