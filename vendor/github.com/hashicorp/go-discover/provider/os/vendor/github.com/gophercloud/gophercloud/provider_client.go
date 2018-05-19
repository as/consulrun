package gophercloud

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

const DefaultUserAgent = "gophercloud/2.0.0"

type UserAgent struct {
	prepend []string
}

func (ua *UserAgent) Prepend(s ...string) {
	ua.prepend = append(s, ua.prepend...)
}

func (ua *UserAgent) Join() string {
	uaSlice := append(ua.prepend, DefaultUserAgent)
	return strings.Join(uaSlice, " ")
}

//
type ProviderClient struct {
	IdentityBase string

	IdentityEndpoint string

	TokenID string

	EndpointLocator EndpointLocator

	HTTPClient http.Client

	UserAgent UserAgent

	ReauthFunc func() error

	Debug bool
}

func (client *ProviderClient) AuthenticatedHeaders() map[string]string {
	if client.TokenID == "" {
		return map[string]string{}
	}
	return map[string]string{"X-Auth-Token": client.TokenID}
}

type RequestOpts struct {
	JSONBody interface{}

	RawBody io.Reader

	JSONResponse interface{}

	OkCodes []int

	MoreHeaders map[string]string

	ErrorContext error
}

var applicationJSON = "application/json"

func (client *ProviderClient) Request(method, url string, options *RequestOpts) (*http.Response, error) {
	var body io.Reader
	var contentType *string

	if options.JSONBody != nil {
		if options.RawBody != nil {
			panic("Please provide only one of JSONBody or RawBody to gophercloud.Request().")
		}

		rendered, err := json.Marshal(options.JSONBody)
		if err != nil {
			return nil, err
		}

		body = bytes.NewReader(rendered)
		contentType = &applicationJSON
	}

	if options.RawBody != nil {
		body = options.RawBody
	}

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	if contentType != nil {
		req.Header.Set("Content-Type", *contentType)
	}
	req.Header.Set("Accept", applicationJSON)

	for k, v := range client.AuthenticatedHeaders() {
		req.Header.Add(k, v)
	}

	req.Header.Set("User-Agent", client.UserAgent.Join())

	if options.MoreHeaders != nil {
		for k, v := range options.MoreHeaders {
			if v != "" {
				req.Header.Set(k, v)
			} else {
				req.Header.Del(k)
			}
		}
	}

	req.Close = true

	resp, err := client.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}

	if options.OkCodes == nil {
		options.OkCodes = defaultOkCodes(method)
	}

	var ok bool
	for _, code := range options.OkCodes {
		if resp.StatusCode == code {
			ok = true
			break
		}
	}

	if !ok {
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		respErr := ErrUnexpectedResponseCode{
			URL:      url,
			Method:   method,
			Expected: options.OkCodes,
			Actual:   resp.StatusCode,
			Body:     body,
		}

		errType := options.ErrorContext
		switch resp.StatusCode {
		case http.StatusBadRequest:
			err = ErrDefault400{respErr}
			if error400er, ok := errType.(Err400er); ok {
				err = error400er.Error400(respErr)
			}
		case http.StatusUnauthorized:
			if client.ReauthFunc != nil {
				err = client.ReauthFunc()
				if err != nil {
					e := &ErrUnableToReauthenticate{}
					e.ErrOriginal = respErr
					return nil, e
				}
				if options.RawBody != nil {
					if seeker, ok := options.RawBody.(io.Seeker); ok {
						seeker.Seek(0, 0)
					}
				}
				resp, err = client.Request(method, url, options)
				if err != nil {
					switch err.(type) {
					case *ErrUnexpectedResponseCode:
						e := &ErrErrorAfterReauthentication{}
						e.ErrOriginal = err.(*ErrUnexpectedResponseCode)
						return nil, e
					default:
						e := &ErrErrorAfterReauthentication{}
						e.ErrOriginal = err
						return nil, e
					}
				}
				return resp, nil
			}
			err = ErrDefault401{respErr}
			if error401er, ok := errType.(Err401er); ok {
				err = error401er.Error401(respErr)
			}
		case http.StatusNotFound:
			err = ErrDefault404{respErr}
			if error404er, ok := errType.(Err404er); ok {
				err = error404er.Error404(respErr)
			}
		case http.StatusMethodNotAllowed:
			err = ErrDefault405{respErr}
			if error405er, ok := errType.(Err405er); ok {
				err = error405er.Error405(respErr)
			}
		case http.StatusRequestTimeout:
			err = ErrDefault408{respErr}
			if error408er, ok := errType.(Err408er); ok {
				err = error408er.Error408(respErr)
			}
		case 429:
			err = ErrDefault429{respErr}
			if error429er, ok := errType.(Err429er); ok {
				err = error429er.Error429(respErr)
			}
		case http.StatusInternalServerError:
			err = ErrDefault500{respErr}
			if error500er, ok := errType.(Err500er); ok {
				err = error500er.Error500(respErr)
			}
		case http.StatusServiceUnavailable:
			err = ErrDefault503{respErr}
			if error503er, ok := errType.(Err503er); ok {
				err = error503er.Error503(respErr)
			}
		}

		if err == nil {
			err = respErr
		}

		return resp, err
	}

	if options.JSONResponse != nil {
		defer resp.Body.Close()
		if err := json.NewDecoder(resp.Body).Decode(options.JSONResponse); err != nil {
			return nil, err
		}
	}

	return resp, nil
}

func defaultOkCodes(method string) []int {
	switch {
	case method == "GET":
		return []int{200}
	case method == "POST":
		return []int{201, 202}
	case method == "PUT":
		return []int{201, 202}
	case method == "PATCH":
		return []int{200, 204}
	case method == "DELETE":
		return []int{202, 204}
	}

	return []int{}
}
