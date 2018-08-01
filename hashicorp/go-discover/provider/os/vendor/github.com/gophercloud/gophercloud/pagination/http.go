package pagination

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/gophercloud/gophercloud"
)

type PageResult struct {
	gophercloud.Result
	url.URL
}

func PageResultFrom(resp *http.Response) (PageResult, error) {
	var parsedBody interface{}

	defer resp.Body.Close()
	rawBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return PageResult{}, err
	}

	if strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		err = json.Unmarshal(rawBody, &parsedBody)
		if err != nil {
			return PageResult{}, err
		}
	} else {
		parsedBody = rawBody
	}

	return PageResultFromParsed(resp, parsedBody), err
}

func PageResultFromParsed(resp *http.Response, body interface{}) PageResult {
	return PageResult{
		Result: gophercloud.Result{
			Body:   body,
			Header: resp.Header,
		},
		URL: *resp.Request.URL,
	}
}

func Request(client *gophercloud.ServiceClient, headers map[string]string, url string) (*http.Response, error) {
	return client.Get(url, nil, &gophercloud.RequestOpts{
		MoreHeaders: headers,
		OkCodes:     []int{200, 204, 300},
	})
}
