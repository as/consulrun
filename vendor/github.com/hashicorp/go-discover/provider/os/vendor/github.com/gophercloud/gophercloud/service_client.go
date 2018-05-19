package gophercloud

import (
	"io"
	"net/http"
	"strings"
)

type ServiceClient struct {
	*ProviderClient

	Endpoint string

	ResourceBase string

	Type string

	Microversion string
}

func (client *ServiceClient) ResourceBaseURL() string {
	if client.ResourceBase != "" {
		return client.ResourceBase
	}
	return client.Endpoint
}

func (client *ServiceClient) ServiceURL(parts ...string) string {
	return client.ResourceBaseURL() + strings.Join(parts, "/")
}

func (client *ServiceClient) initReqOpts(url string, JSONBody interface{}, JSONResponse interface{}, opts *RequestOpts) {
	if v, ok := (JSONBody).(io.Reader); ok {
		opts.RawBody = v
	} else if JSONBody != nil {
		opts.JSONBody = JSONBody
	}

	if JSONResponse != nil {
		opts.JSONResponse = JSONResponse
	}

	if opts.MoreHeaders == nil {
		opts.MoreHeaders = make(map[string]string)
	}

	if client.Microversion != "" {
		client.setMicroversionHeader(opts)
	}
}

func (client *ServiceClient) Get(url string, JSONResponse interface{}, opts *RequestOpts) (*http.Response, error) {
	if opts == nil {
		opts = new(RequestOpts)
	}
	client.initReqOpts(url, nil, JSONResponse, opts)
	return client.Request("GET", url, opts)
}

func (client *ServiceClient) Post(url string, JSONBody interface{}, JSONResponse interface{}, opts *RequestOpts) (*http.Response, error) {
	if opts == nil {
		opts = new(RequestOpts)
	}
	client.initReqOpts(url, JSONBody, JSONResponse, opts)
	return client.Request("POST", url, opts)
}

func (client *ServiceClient) Put(url string, JSONBody interface{}, JSONResponse interface{}, opts *RequestOpts) (*http.Response, error) {
	if opts == nil {
		opts = new(RequestOpts)
	}
	client.initReqOpts(url, JSONBody, JSONResponse, opts)
	return client.Request("PUT", url, opts)
}

func (client *ServiceClient) Patch(url string, JSONBody interface{}, JSONResponse interface{}, opts *RequestOpts) (*http.Response, error) {
	if opts == nil {
		opts = new(RequestOpts)
	}
	client.initReqOpts(url, JSONBody, JSONResponse, opts)
	return client.Request("PATCH", url, opts)
}

func (client *ServiceClient) Delete(url string, opts *RequestOpts) (*http.Response, error) {
	if opts == nil {
		opts = new(RequestOpts)
	}
	client.initReqOpts(url, nil, nil, opts)
	return client.Request("DELETE", url, opts)
}

func (client *ServiceClient) setMicroversionHeader(opts *RequestOpts) {
	switch client.Type {
	case "compute":
		opts.MoreHeaders["X-OpenStack-Nova-API-Version"] = client.Microversion
	case "sharev2":
		opts.MoreHeaders["X-OpenStack-Manila-API-Version"] = client.Microversion
	}

	if client.Type != "" {
		opts.MoreHeaders["OpenStack-API-Version"] = client.Type + " " + client.Microversion
	}
}
