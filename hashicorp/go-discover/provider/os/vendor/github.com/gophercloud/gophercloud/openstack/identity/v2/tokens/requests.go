package tokens

import "github.com/gophercloud/gophercloud"

type PasswordCredentialsV2 struct {
	Username string `json:"username" required:"true"`
	Password string `json:"password" required:"true"`
}

type TokenCredentialsV2 struct {
	ID string `json:"id,omitempty" required:"true"`
}

type AuthOptionsV2 struct {
	PasswordCredentials *PasswordCredentialsV2 `json:"passwordCredentials,omitempty" xor:"TokenCredentials"`

	TenantID   string `json:"tenantId,omitempty"`
	TenantName string `json:"tenantName,omitempty"`

	TokenCredentials *TokenCredentialsV2 `json:"token,omitempty" xor:"PasswordCredentials"`
}

type AuthOptionsBuilder interface {
	ToTokenV2CreateMap() (map[string]interface{}, error)
}

// AuthOptions are the valid options for Openstack Identity v2 authentication.
type AuthOptions struct {
	IdentityEndpoint string `json:"-"`
	Username         string `json:"username,omitempty"`
	Password         string `json:"password,omitempty"`
	TenantID         string `json:"tenantId,omitempty"`
	TenantName       string `json:"tenantName,omitempty"`
	AllowReauth      bool   `json:"-"`
	TokenID          string
}

// ToTokenV2CreateMap builds a token request body from the given AuthOptions.
func (opts AuthOptions) ToTokenV2CreateMap() (map[string]interface{}, error) {
	v2Opts := AuthOptionsV2{
		TenantID:   opts.TenantID,
		TenantName: opts.TenantName,
	}

	if opts.Password != "" {
		v2Opts.PasswordCredentials = &PasswordCredentialsV2{
			Username: opts.Username,
			Password: opts.Password,
		}
	} else {
		v2Opts.TokenCredentials = &TokenCredentialsV2{
			ID: opts.TokenID,
		}
	}

	b, err := gophercloud.BuildRequestBody(v2Opts, "auth")
	if err != nil {
		return nil, err
	}
	return b, nil
}

// call openstack.AuthenticatedClient(), which abstracts all of the gory details
func Create(client *gophercloud.ServiceClient, auth AuthOptionsBuilder) (r CreateResult) {
	b, err := auth.ToTokenV2CreateMap()
	if err != nil {
		r.Err = err
		return
	}
	_, r.Err = client.Post(CreateURL(client), b, &r.Body, &gophercloud.RequestOpts{
		OkCodes:     []int{200, 203},
		MoreHeaders: map[string]string{"X-Auth-Token": ""},
	})
	return
}

func Get(client *gophercloud.ServiceClient, token string) (r GetResult) {
	_, r.Err = client.Get(GetURL(client, token), &r.Body, &gophercloud.RequestOpts{
		OkCodes: []int{200, 203},
	})
	return
}
