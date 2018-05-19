// Copyright 2016 Circonus, Inc. All rights reserved.

package api

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

const (
	defaultAPIURL = "https://api.circonus.com/v2"
	defaultAPIApp = "circonus-gometrics"
	minRetryWait  = 1 * time.Second
	maxRetryWait  = 15 * time.Second
	maxRetries    = 4 // equating to 1 + maxRetries total attempts
)

type TokenKeyType string

type TokenAppType string

type IDType int

type CIDType string

type URLType string

type SearchQueryType string

type SearchFilterType string

type TagType []string

type Config struct {
	URL      string
	TokenKey string
	TokenApp string
	Log      *log.Logger
	Debug    bool
}

type API struct {
	apiURL *url.URL
	key    TokenKeyType
	app    TokenAppType
	Debug  bool
	Log    *log.Logger
}

func NewAPI(ac *Config) (*API, error) {

	if ac == nil {
		return nil, errors.New("Invalid API configuration (nil)")
	}

	key := TokenKeyType(ac.TokenKey)
	if key == "" {
		return nil, errors.New("API Token is required")
	}

	app := TokenAppType(ac.TokenApp)
	if app == "" {
		app = defaultAPIApp
	}

	au := string(ac.URL)
	if au == "" {
		au = defaultAPIURL
	}
	if !strings.Contains(au, "/") {

		au = fmt.Sprintf("https://%s/v2", ac.URL)
	}
	if last := len(au) - 1; last >= 0 && au[last] == '/' {
		au = au[:last]
	}
	apiURL, err := url.Parse(au)
	if err != nil {
		return nil, err
	}

	a := &API{apiURL, key, app, ac.Debug, ac.Log}

	a.Debug = ac.Debug
	a.Log = ac.Log
	if a.Debug && a.Log == nil {
		a.Log = log.New(os.Stderr, "", log.LstdFlags)
	}
	if a.Log == nil {
		a.Log = log.New(ioutil.Discard, "", log.LstdFlags)
	}

	return a, nil
}

func (a *API) Get(reqPath string) ([]byte, error) {
	return a.apiCall("GET", reqPath, nil)
}

func (a *API) Delete(reqPath string) ([]byte, error) {
	return a.apiCall("DELETE", reqPath, nil)
}

func (a *API) Post(reqPath string, data []byte) ([]byte, error) {
	return a.apiCall("POST", reqPath, data)
}

func (a *API) Put(reqPath string, data []byte) ([]byte, error) {
	return a.apiCall("PUT", reqPath, data)
}

func (a *API) apiCall(reqMethod string, reqPath string, data []byte) ([]byte, error) {
	dataReader := bytes.NewReader(data)
	reqURL := a.apiURL.String()

	if reqPath[:1] != "/" {
		reqURL += "/"
	}
	if reqPath[:3] == "/v2" {
		reqURL += reqPath[3:]
	} else {
		reqURL += reqPath
	}

	req, err := retryablehttp.NewRequest(reqMethod, reqURL, dataReader)
	if err != nil {
		return nil, fmt.Errorf("[ERROR] creating API request: %s %+v", reqURL, err)
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("X-Circonus-Auth-Token", string(a.key))
	req.Header.Add("X-Circonus-App-Name", string(a.app))

	var lastHTTPError error
	retryPolicy := func(resp *http.Response, err error) (bool, error) {
		if err != nil {
			lastHTTPError = err
			return true, err
		}

		if resp.StatusCode == 0 || resp.StatusCode >= 500 || resp.StatusCode == 429 {
			body, readErr := ioutil.ReadAll(resp.Body)
			if readErr != nil {
				lastHTTPError = fmt.Errorf("- last HTTP error: %d %+v", resp.StatusCode, readErr)
			} else {
				lastHTTPError = fmt.Errorf("- last HTTP error: %d %s", resp.StatusCode, string(body))
			}
			return true, nil
		}
		return false, nil
	}

	client := retryablehttp.NewClient()
	client.RetryWaitMin = minRetryWait
	client.RetryWaitMax = maxRetryWait
	client.RetryMax = maxRetries

	if a.Debug {
		client.Logger = a.Log
	} else {
		client.Logger = log.New(ioutil.Discard, "", log.LstdFlags)
	}

	client.CheckRetry = retryPolicy

	resp, err := client.Do(req)
	if err != nil {
		if lastHTTPError != nil {
			return nil, lastHTTPError
		}
		return nil, fmt.Errorf("[ERROR] %s: %+v", reqURL, err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("[ERROR] reading response %+v", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := fmt.Sprintf("API response code %d: %s", resp.StatusCode, string(body))
		if a.Debug {
			a.Log.Printf("[DEBUG] %s\n", msg)
		}

		return nil, fmt.Errorf("[ERROR] %s", msg)
	}

	return body, nil
}
