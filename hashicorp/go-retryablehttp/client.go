// The retryablehttp package provides a familiar HTTP client interface with
//
//
package retryablehttp

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/as/consulrun/hashicorp/go-cleanhttp"
)

var (
	defaultRetryWaitMin = 1 * time.Second
	defaultRetryWaitMax = 30 * time.Second
	defaultRetryMax     = 4

	defaultClient = NewClient()

	respReadLimit = int64(4096)
)

type LenReader interface {
	Len() int
}

type Request struct {
	body io.ReadSeeker

	*http.Request
}

func NewRequest(method, url string, body io.ReadSeeker) (*Request, error) {

	var rcBody io.ReadCloser
	if body != nil {
		rcBody = ioutil.NopCloser(body)
	}

	httpReq, err := http.NewRequest(method, url, rcBody)
	if err != nil {
		return nil, err
	}

	if lr, ok := body.(LenReader); ok {
		httpReq.ContentLength = int64(lr.Len())
	}

	return &Request{body, httpReq}, nil
}

type RequestLogHook func(*log.Logger, *http.Request, int)

type ResponseLogHook func(*log.Logger, *http.Response)

type CheckRetry func(resp *http.Response, err error) (bool, error)

type Backoff func(min, max time.Duration, attemptNum int, resp *http.Response) time.Duration

type Client struct {
	HTTPClient *http.Client // Internal HTTP client.
	Logger     *log.Logger  // Customer logger instance.

	RetryWaitMin time.Duration // Minimum time to wait
	RetryWaitMax time.Duration // Maximum time to wait
	RetryMax     int           // Maximum number of retries

	RequestLogHook RequestLogHook

	ResponseLogHook ResponseLogHook

	CheckRetry CheckRetry

	Backoff Backoff
}

func NewClient() *Client {
	return &Client{
		HTTPClient:   cleanhttp.DefaultClient(),
		Logger:       log.New(os.Stderr, "", log.LstdFlags),
		RetryWaitMin: defaultRetryWaitMin,
		RetryWaitMax: defaultRetryWaitMax,
		RetryMax:     defaultRetryMax,
		CheckRetry:   DefaultRetryPolicy,
		Backoff:      DefaultBackoff,
	}
}

func DefaultRetryPolicy(resp *http.Response, err error) (bool, error) {
	if err != nil {
		return true, err
	}

	if resp.StatusCode == 0 || resp.StatusCode >= 500 {
		return true, nil
	}

	return false, nil
}

func DefaultBackoff(min, max time.Duration, attemptNum int, resp *http.Response) time.Duration {
	mult := math.Pow(2, float64(attemptNum)) * float64(min)
	sleep := time.Duration(mult)
	if float64(sleep) != mult || sleep > max {
		sleep = max
	}
	return sleep
}

func (c *Client) Do(req *Request) (*http.Response, error) {
	c.Logger.Printf("[DEBUG] %s %s", req.Method, req.URL)

	for i := 0; ; i++ {
		var code int // HTTP response code

		if req.body != nil {
			if _, err := req.body.Seek(0, 0); err != nil {
				return nil, fmt.Errorf("failed to seek body: %v", err)
			}
		}

		if c.RequestLogHook != nil {
			c.RequestLogHook(c.Logger, req.Request, i)
		}

		resp, err := c.HTTPClient.Do(req.Request)

		checkOK, checkErr := c.CheckRetry(resp, err)

		if err != nil {
			c.Logger.Printf("[ERR] %s %s request failed: %v", req.Method, req.URL, err)
		} else {

			if c.ResponseLogHook != nil {

				c.ResponseLogHook(c.Logger, resp)
			}
		}

		if !checkOK {
			if checkErr != nil {
				err = checkErr
			}
			return resp, err
		}

		if err == nil {
			c.drainBody(resp.Body)
		}

		remain := c.RetryMax - i
		if remain == 0 {
			break
		}
		wait := c.Backoff(c.RetryWaitMin, c.RetryWaitMax, i, resp)
		desc := fmt.Sprintf("%s %s", req.Method, req.URL)
		if code > 0 {
			desc = fmt.Sprintf("%s (status: %d)", desc, code)
		}
		c.Logger.Printf("[DEBUG] %s: retrying in %s (%d left)", desc, wait, remain)
		time.Sleep(wait)
	}

	return nil, fmt.Errorf("%s %s giving up after %d attempts",
		req.Method, req.URL, c.RetryMax+1)
}

func (c *Client) drainBody(body io.ReadCloser) {
	defer body.Close()
	_, err := io.Copy(ioutil.Discard, io.LimitReader(body, respReadLimit))
	if err != nil {
		c.Logger.Printf("[ERR] error reading response body: %v", err)
	}
}

func Get(url string) (*http.Response, error) {
	return defaultClient.Get(url)
}

func (c *Client) Get(url string) (*http.Response, error) {
	req, err := NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

func Head(url string) (*http.Response, error) {
	return defaultClient.Head(url)
}

func (c *Client) Head(url string) (*http.Response, error) {
	req, err := NewRequest("HEAD", url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

func Post(url, bodyType string, body io.ReadSeeker) (*http.Response, error) {
	return defaultClient.Post(url, bodyType, body)
}

func (c *Client) Post(url, bodyType string, body io.ReadSeeker) (*http.Response, error) {
	req, err := NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", bodyType)
	return c.Do(req)
}

func PostForm(url string, data url.Values) (*http.Response, error) {
	return defaultClient.PostForm(url, data)
}

func (c *Client) PostForm(url string, data url.Values) (*http.Response, error) {
	return c.Post(url, "application/x-www-form-urlencoded", strings.NewReader(data.Encode()))
}
