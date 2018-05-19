package flavors

import (
	"encoding/json"
	"strconv"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/pagination"
)

type commonResult struct {
	gophercloud.Result
}

type CreateResult struct {
	commonResult
}

type GetResult struct {
	commonResult
}

type DeleteResult struct {
	gophercloud.ErrResult
}

func (r commonResult) Extract() (*Flavor, error) {
	var s struct {
		Flavor *Flavor `json:"flavor"`
	}
	err := r.ExtractInto(&s)
	return s.Flavor, err
}

type Flavor struct {
	ID string `json:"id"`

	Disk int `json:"disk"`

	RAM int `json:"ram"`

	Name string `json:"name"`

	RxTxFactor float64 `json:"rxtx_factor"`

	Swap int `json:"swap"`

	VCPUs int `json:"vcpus"`

	IsPublic bool `json:"is_public"`
}

func (r *Flavor) UnmarshalJSON(b []byte) error {
	type tmp Flavor
	var s struct {
		tmp
		Swap interface{} `json:"swap"`
	}
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	*r = Flavor(s.tmp)

	switch t := s.Swap.(type) {
	case float64:
		r.Swap = int(t)
	case string:
		switch t {
		case "":
			r.Swap = 0
		default:
			swap, err := strconv.ParseFloat(t, 64)
			if err != nil {
				return err
			}
			r.Swap = int(swap)
		}
	}

	return nil
}

type FlavorPage struct {
	pagination.LinkedPageBase
}

func (page FlavorPage) IsEmpty() (bool, error) {
	flavors, err := ExtractFlavors(page)
	return len(flavors) == 0, err
}

func (page FlavorPage) NextPageURL() (string, error) {
	var s struct {
		Links []gophercloud.Link `json:"flavors_links"`
	}
	err := page.ExtractInto(&s)
	if err != nil {
		return "", err
	}
	return gophercloud.ExtractNextURL(s.Links)
}

func ExtractFlavors(r pagination.Page) ([]Flavor, error) {
	var s struct {
		Flavors []Flavor `json:"flavors"`
	}
	err := (r.(FlavorPage)).ExtractInto(&s)
	return s.Flavors, err
}
