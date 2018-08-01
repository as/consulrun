package tokens

import "github.com/gophercloud/gophercloud"

type Scope struct {
	ProjectID   string
	ProjectName string
	DomainID    string
	DomainName  string
}

type AuthOptionsBuilder interface {
	ToTokenV3CreateMap(map[string]interface{}) (map[string]interface{}, error)
	ToTokenV3ScopeMap() (map[string]interface{}, error)
	CanReauth() bool
}

type AuthOptions struct {
	IdentityEndpoint string `json:"-"`

	Username string `json:"username,omitempty"`
	UserID   string `json:"id,omitempty"`

	Password string `json:"password,omitempty"`

	DomainID   string `json:"-"`
	DomainName string `json:"name,omitempty"`

	AllowReauth bool `json:"-"`

	TokenID string `json:"-"`

	Scope Scope `json:"-"`
}

// ToTokenV3CreateMap builds a request body from AuthOptions.
func (opts *AuthOptions) ToTokenV3CreateMap(scope map[string]interface{}) (map[string]interface{}, error) {
	gophercloudAuthOpts := gophercloud.AuthOptions{
		Username:    opts.Username,
		UserID:      opts.UserID,
		Password:    opts.Password,
		DomainID:    opts.DomainID,
		DomainName:  opts.DomainName,
		AllowReauth: opts.AllowReauth,
		TokenID:     opts.TokenID,
	}

	return gophercloudAuthOpts.ToTokenV3CreateMap(scope)
}

// ToTokenV3CreateMap builds a scope request body from AuthOptions.
func (opts *AuthOptions) ToTokenV3ScopeMap() (map[string]interface{}, error) {
	if opts.Scope.ProjectName != "" {

		if opts.Scope.DomainID == "" && opts.Scope.DomainName == "" {
			return nil, gophercloud.ErrScopeDomainIDOrDomainName{}
		}
		if opts.Scope.ProjectID != "" {
			return nil, gophercloud.ErrScopeProjectIDOrProjectName{}
		}

		if opts.Scope.DomainID != "" {

			return map[string]interface{}{
				"project": map[string]interface{}{
					"name":   &opts.Scope.ProjectName,
					"domain": map[string]interface{}{"id": &opts.Scope.DomainID},
				},
			}, nil
		}

		if opts.Scope.DomainName != "" {

			return map[string]interface{}{
				"project": map[string]interface{}{
					"name":   &opts.Scope.ProjectName,
					"domain": map[string]interface{}{"name": &opts.Scope.DomainName},
				},
			}, nil
		}
	} else if opts.Scope.ProjectID != "" {

		if opts.Scope.DomainID != "" {
			return nil, gophercloud.ErrScopeProjectIDAlone{}
		}
		if opts.Scope.DomainName != "" {
			return nil, gophercloud.ErrScopeProjectIDAlone{}
		}

		return map[string]interface{}{
			"project": map[string]interface{}{
				"id": &opts.Scope.ProjectID,
			},
		}, nil
	} else if opts.Scope.DomainID != "" {

		if opts.Scope.DomainName != "" {
			return nil, gophercloud.ErrScopeDomainIDOrDomainName{}
		}

		return map[string]interface{}{
			"domain": map[string]interface{}{
				"id": &opts.Scope.DomainID,
			},
		}, nil
	} else if opts.Scope.DomainName != "" {

		return map[string]interface{}{
			"domain": map[string]interface{}{
				"name": &opts.Scope.DomainName,
			},
		}, nil
	}

	return nil, nil
}

func (opts *AuthOptions) CanReauth() bool {
	return opts.AllowReauth
}

func subjectTokenHeaders(c *gophercloud.ServiceClient, subjectToken string) map[string]string {
	return map[string]string{
		"X-Subject-Token": subjectToken,
	}
}

func Create(c *gophercloud.ServiceClient, opts AuthOptionsBuilder) (r CreateResult) {
	scope, err := opts.ToTokenV3ScopeMap()
	if err != nil {
		r.Err = err
		return
	}

	b, err := opts.ToTokenV3CreateMap(scope)
	if err != nil {
		r.Err = err
		return
	}

	resp, err := c.Post(tokenURL(c), b, &r.Body, &gophercloud.RequestOpts{
		MoreHeaders: map[string]string{"X-Auth-Token": ""},
	})
	r.Err = err
	if resp != nil {
		r.Header = resp.Header
	}
	return
}

func Get(c *gophercloud.ServiceClient, token string) (r GetResult) {
	resp, err := c.Get(tokenURL(c), &r.Body, &gophercloud.RequestOpts{
		MoreHeaders: subjectTokenHeaders(c, token),
		OkCodes:     []int{200, 203},
	})
	if resp != nil {
		r.Err = err
		r.Header = resp.Header
	}
	return
}

func Validate(c *gophercloud.ServiceClient, token string) (bool, error) {
	resp, err := c.Request("HEAD", tokenURL(c), &gophercloud.RequestOpts{
		MoreHeaders: subjectTokenHeaders(c, token),
		OkCodes:     []int{200, 204, 404},
	})
	if err != nil {
		return false, err
	}

	return resp.StatusCode == 200 || resp.StatusCode == 204, nil
}

func Revoke(c *gophercloud.ServiceClient, token string) (r RevokeResult) {
	_, r.Err = c.Delete(tokenURL(c), &gophercloud.RequestOpts{
		MoreHeaders: subjectTokenHeaders(c, token),
	})
	return
}
