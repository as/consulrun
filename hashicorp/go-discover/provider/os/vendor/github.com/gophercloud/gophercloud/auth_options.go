package gophercloud

/*
AuthOptions stores information needed to authenticate to an OpenStack Cloud.
You can populate one manually, or use a provider's AuthOptionsFromEnv() function
to read relevant information from the standard environment variables. Pass one
to a provider's AuthenticatedClient function to authenticate and obtain a
ProviderClient representing an active session on that provider.

Its fields are the union of those recognized by each identity implementation and
provider.

An example of manually providing authentication information:

  opts := gophercloud.AuthOptions{
    IdentityEndpoint: "https://openstack.example.com:5000/v2.0",
    Username: "{username}",
    Password: "{password}",
    TenantID: "{tenant_id}",
  }

  provider, err := openstack.AuthenticatedClient(opts)

An example of using AuthOptionsFromEnv(), where the environment variables can
be read from a file, such as a standard openrc file:

  opts, err := openstack.AuthOptionsFromEnv()
  provider, err := openstack.AuthenticatedClient(opts)
*/
type AuthOptions struct {

	//

	IdentityEndpoint string `json:"-"`

	Username string `json:"username,omitempty"`
	UserID   string `json:"-"`

	Password string `json:"password,omitempty"`

	DomainID   string `json:"-"`
	DomainName string `json:"name,omitempty"`

	TenantID   string `json:"tenantId,omitempty"`
	TenantName string `json:"tenantName,omitempty"`

	//

	AllowReauth bool `json:"-"`

	TokenID string `json:"-"`
}

func (opts AuthOptions) ToTokenV2CreateMap() (map[string]interface{}, error) {

	authMap := make(map[string]interface{})

	if opts.Username != "" {
		if opts.Password != "" {
			authMap["passwordCredentials"] = map[string]interface{}{
				"username": opts.Username,
				"password": opts.Password,
			}
		} else {
			return nil, ErrMissingInput{Argument: "Password"}
		}
	} else if opts.TokenID != "" {
		authMap["token"] = map[string]interface{}{
			"id": opts.TokenID,
		}
	} else {
		return nil, ErrMissingInput{Argument: "Username"}
	}

	if opts.TenantID != "" {
		authMap["tenantId"] = opts.TenantID
	}
	if opts.TenantName != "" {
		authMap["tenantName"] = opts.TenantName
	}

	return map[string]interface{}{"auth": authMap}, nil
}

func (opts *AuthOptions) ToTokenV3CreateMap(scope map[string]interface{}) (map[string]interface{}, error) {
	type domainReq struct {
		ID   *string `json:"id,omitempty"`
		Name *string `json:"name,omitempty"`
	}

	type projectReq struct {
		Domain *domainReq `json:"domain,omitempty"`
		Name   *string    `json:"name,omitempty"`
		ID     *string    `json:"id,omitempty"`
	}

	type userReq struct {
		ID       *string    `json:"id,omitempty"`
		Name     *string    `json:"name,omitempty"`
		Password string     `json:"password"`
		Domain   *domainReq `json:"domain,omitempty"`
	}

	type passwordReq struct {
		User userReq `json:"user"`
	}

	type tokenReq struct {
		ID string `json:"id"`
	}

	type identityReq struct {
		Methods  []string     `json:"methods"`
		Password *passwordReq `json:"password,omitempty"`
		Token    *tokenReq    `json:"token,omitempty"`
	}

	type authReq struct {
		Identity identityReq `json:"identity"`
	}

	type request struct {
		Auth authReq `json:"auth"`
	}

	var req request

	if opts.Password == "" {
		if opts.TokenID != "" {

			if opts.Username != "" {
				return nil, ErrUsernameWithToken{}
			}
			if opts.UserID != "" {
				return nil, ErrUserIDWithToken{}
			}
			if opts.DomainID != "" {
				return nil, ErrDomainIDWithToken{}
			}
			if opts.DomainName != "" {
				return nil, ErrDomainNameWithToken{}
			}

			req.Auth.Identity.Methods = []string{"token"}
			req.Auth.Identity.Token = &tokenReq{
				ID: opts.TokenID,
			}
		} else {

			return nil, ErrMissingPassword{}
		}
	} else {

		req.Auth.Identity.Methods = []string{"password"}

		if opts.Username == "" && opts.UserID == "" {
			return nil, ErrUsernameOrUserID{}
		}

		if opts.Username != "" {

			if opts.UserID != "" {
				return nil, ErrUsernameOrUserID{}
			}

			if opts.DomainID == "" && opts.DomainName == "" {
				return nil, ErrDomainIDOrDomainName{}
			}

			if opts.DomainID != "" {
				if opts.DomainName != "" {
					return nil, ErrDomainIDOrDomainName{}
				}

				req.Auth.Identity.Password = &passwordReq{
					User: userReq{
						Name:     &opts.Username,
						Password: opts.Password,
						Domain:   &domainReq{ID: &opts.DomainID},
					},
				}
			}

			if opts.DomainName != "" {

				req.Auth.Identity.Password = &passwordReq{
					User: userReq{
						Name:     &opts.Username,
						Password: opts.Password,
						Domain:   &domainReq{Name: &opts.DomainName},
					},
				}
			}
		}

		if opts.UserID != "" {

			if opts.DomainID != "" {
				return nil, ErrDomainIDWithUserID{}
			}
			if opts.DomainName != "" {
				return nil, ErrDomainNameWithUserID{}
			}

			req.Auth.Identity.Password = &passwordReq{
				User: userReq{ID: &opts.UserID, Password: opts.Password},
			}
		}
	}

	b, err := BuildRequestBody(req, "")
	if err != nil {
		return nil, err
	}

	if len(scope) != 0 {
		b["auth"].(map[string]interface{})["scope"] = scope
	}

	return b, nil
}

func (opts *AuthOptions) ToTokenV3ScopeMap() (map[string]interface{}, error) {

	var scope struct {
		ProjectID   string
		ProjectName string
		DomainID    string
		DomainName  string
	}

	if opts.TenantID != "" {
		scope.ProjectID = opts.TenantID
	} else {
		if opts.TenantName != "" {
			scope.ProjectName = opts.TenantName
			scope.DomainID = opts.DomainID
			scope.DomainName = opts.DomainName
		}
	}

	if scope.ProjectName != "" {

		if scope.DomainID == "" && scope.DomainName == "" {
			return nil, ErrScopeDomainIDOrDomainName{}
		}
		if scope.ProjectID != "" {
			return nil, ErrScopeProjectIDOrProjectName{}
		}

		if scope.DomainID != "" {

			return map[string]interface{}{
				"project": map[string]interface{}{
					"name":   &scope.ProjectName,
					"domain": map[string]interface{}{"id": &scope.DomainID},
				},
			}, nil
		}

		if scope.DomainName != "" {

			return map[string]interface{}{
				"project": map[string]interface{}{
					"name":   &scope.ProjectName,
					"domain": map[string]interface{}{"name": &scope.DomainName},
				},
			}, nil
		}
	} else if scope.ProjectID != "" {

		if scope.DomainID != "" {
			return nil, ErrScopeProjectIDAlone{}
		}
		if scope.DomainName != "" {
			return nil, ErrScopeProjectIDAlone{}
		}

		return map[string]interface{}{
			"project": map[string]interface{}{
				"id": &scope.ProjectID,
			},
		}, nil
	} else if scope.DomainID != "" {

		if scope.DomainName != "" {
			return nil, ErrScopeDomainIDOrDomainName{}
		}

		return map[string]interface{}{
			"domain": map[string]interface{}{
				"id": &scope.DomainID,
			},
		}, nil
	} else if scope.DomainName != "" {

		return map[string]interface{}{
			"domain": map[string]interface{}{
				"name": &scope.DomainName,
			},
		}, nil
	}

	return nil, nil
}

func (opts AuthOptions) CanReauth() bool {
	return opts.AllowReauth
}
