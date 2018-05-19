package servers

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"time"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/pagination"
)

type serverResult struct {
	gophercloud.Result
}

func (r serverResult) Extract() (*Server, error) {
	var s Server
	err := r.ExtractInto(&s)
	return &s, err
}

func (r serverResult) ExtractInto(v interface{}) error {
	return r.Result.ExtractIntoStructPtr(v, "server")
}

func ExtractServersInto(r pagination.Page, v interface{}) error {
	return r.(ServerPage).Result.ExtractIntoSlicePtr(v, "servers")
}

type CreateResult struct {
	serverResult
}

type GetResult struct {
	serverResult
}

type UpdateResult struct {
	serverResult
}

type DeleteResult struct {
	gophercloud.ErrResult
}

// RebuildResult is the response from a Rebuild operation. Call its Extract
type RebuildResult struct {
	serverResult
}

type ActionResult struct {
	gophercloud.ErrResult
}

type RescueResult struct {
	ActionResult
}

type CreateImageResult struct {
	gophercloud.Result
}

type GetPasswordResult struct {
	gophercloud.Result
}

func (r GetPasswordResult) ExtractPassword(privateKey *rsa.PrivateKey) (string, error) {
	var s struct {
		Password string `json:"password"`
	}
	err := r.ExtractInto(&s)
	if err == nil && privateKey != nil && s.Password != "" {
		return decryptPassword(s.Password, privateKey)
	}
	return s.Password, err
}

func decryptPassword(encryptedPassword string, privateKey *rsa.PrivateKey) (string, error) {
	b64EncryptedPassword := make([]byte, base64.StdEncoding.DecodedLen(len(encryptedPassword)))

	n, err := base64.StdEncoding.Decode(b64EncryptedPassword, []byte(encryptedPassword))
	if err != nil {
		return "", fmt.Errorf("Failed to base64 decode encrypted password: %s", err)
	}
	password, err := rsa.DecryptPKCS1v15(nil, privateKey, b64EncryptedPassword[0:n])
	if err != nil {
		return "", fmt.Errorf("Failed to decrypt password: %s", err)
	}

	return string(password), nil
}

func (r CreateImageResult) ExtractImageID() (string, error) {
	if r.Err != nil {
		return "", r.Err
	}

	u, err := url.ParseRequestURI(r.Header.Get("Location"))
	if err != nil {
		return "", err
	}
	imageID := path.Base(u.Path)
	if imageID == "." || imageID == "/" {
		return "", fmt.Errorf("Failed to parse the ID of newly created image: %s", u)
	}
	return imageID, nil
}

func (r RescueResult) Extract() (string, error) {
	var s struct {
		AdminPass string `json:"adminPass"`
	}
	err := r.ExtractInto(&s)
	return s.AdminPass, err
}

type Server struct {
	ID string `json:"id"`

	TenantID string `json:"tenant_id"`

	UserID string `json:"user_id"`

	Name string `json:"name"`

	Updated time.Time `json:"updated"`
	Created time.Time `json:"created"`

	HostID string `json:"hostid"`

	Status string `json:"status"`

	Progress int `json:"progress"`

	AccessIPv4 string `json:"accessIPv4"`
	AccessIPv6 string `json:"accessIPv6"`

	Image map[string]interface{} `json:"-"`

	Flavor map[string]interface{} `json:"flavor"`

	Addresses map[string]interface{} `json:"addresses"`

	Metadata map[string]string `json:"metadata"`

	Links []interface{} `json:"links"`

	KeyName string `json:"key_name"`

	AdminPass string `json:"adminPass"`

	SecurityGroups []map[string]interface{} `json:"security_groups"`
}

func (r *Server) UnmarshalJSON(b []byte) error {
	type tmp Server
	var s struct {
		tmp
		Image interface{} `json:"image"`
	}
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	*r = Server(s.tmp)

	switch t := s.Image.(type) {
	case map[string]interface{}:
		r.Image = t
	case string:
		switch t {
		case "":
			r.Image = nil
		}
	}

	return err
}

type ServerPage struct {
	pagination.LinkedPageBase
}

func (r ServerPage) IsEmpty() (bool, error) {
	s, err := ExtractServers(r)
	return len(s) == 0, err
}

func (r ServerPage) NextPageURL() (string, error) {
	var s struct {
		Links []gophercloud.Link `json:"servers_links"`
	}
	err := r.ExtractInto(&s)
	if err != nil {
		return "", err
	}
	return gophercloud.ExtractNextURL(s.Links)
}

func ExtractServers(r pagination.Page) ([]Server, error) {
	var s []Server
	err := ExtractServersInto(r, &s)
	return s, err
}

type MetadataResult struct {
	gophercloud.Result
}

type GetMetadataResult struct {
	MetadataResult
}

type ResetMetadataResult struct {
	MetadataResult
}

type UpdateMetadataResult struct {
	MetadataResult
}

type MetadatumResult struct {
	gophercloud.Result
}

type GetMetadatumResult struct {
	MetadatumResult
}

type CreateMetadatumResult struct {
	MetadatumResult
}

type DeleteMetadatumResult struct {
	gophercloud.ErrResult
}

func (r MetadataResult) Extract() (map[string]string, error) {
	var s struct {
		Metadata map[string]string `json:"metadata"`
	}
	err := r.ExtractInto(&s)
	return s.Metadata, err
}

func (r MetadatumResult) Extract() (map[string]string, error) {
	var s struct {
		Metadatum map[string]string `json:"meta"`
	}
	err := r.ExtractInto(&s)
	return s.Metadatum, err
}

type Address struct {
	Version int    `json:"version"`
	Address string `json:"addr"`
}

type AddressPage struct {
	pagination.SinglePageBase
}

func (r AddressPage) IsEmpty() (bool, error) {
	addresses, err := ExtractAddresses(r)
	return len(addresses) == 0, err
}

func ExtractAddresses(r pagination.Page) (map[string][]Address, error) {
	var s struct {
		Addresses map[string][]Address `json:"addresses"`
	}
	err := (r.(AddressPage)).ExtractInto(&s)
	return s.Addresses, err
}

type NetworkAddressPage struct {
	pagination.SinglePageBase
}

func (r NetworkAddressPage) IsEmpty() (bool, error) {
	addresses, err := ExtractNetworkAddresses(r)
	return len(addresses) == 0, err
}

func ExtractNetworkAddresses(r pagination.Page) ([]Address, error) {
	var s map[string][]Address
	err := (r.(NetworkAddressPage)).ExtractInto(&s)
	if err != nil {
		return nil, err
	}

	var key string
	for k := range s {
		key = k
	}

	return s[key], err
}
