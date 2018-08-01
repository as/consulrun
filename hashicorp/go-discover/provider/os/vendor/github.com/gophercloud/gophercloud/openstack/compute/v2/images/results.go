package images

import (
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/pagination"
)

type GetResult struct {
	gophercloud.Result
}

type DeleteResult struct {
	gophercloud.ErrResult
}

func (r GetResult) Extract() (*Image, error) {
	var s struct {
		Image *Image `json:"image"`
	}
	err := r.ExtractInto(&s)
	return s.Image, err
}

type Image struct {
	ID string

	Created string

	MinDisk int

	MinRAM int

	Name string

	Progress int

	Status string

	Updated string

	Metadata map[string]interface{}
}

type ImagePage struct {
	pagination.LinkedPageBase
}

func (page ImagePage) IsEmpty() (bool, error) {
	images, err := ExtractImages(page)
	return len(images) == 0, err
}

func (page ImagePage) NextPageURL() (string, error) {
	var s struct {
		Links []gophercloud.Link `json:"images_links"`
	}
	err := page.ExtractInto(&s)
	if err != nil {
		return "", err
	}
	return gophercloud.ExtractNextURL(s.Links)
}

func ExtractImages(r pagination.Page) ([]Image, error) {
	var s struct {
		Images []Image `json:"images"`
	}
	err := (r.(ImagePage)).ExtractInto(&s)
	return s.Images, err
}
