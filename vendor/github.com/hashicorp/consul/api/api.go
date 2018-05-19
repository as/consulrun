package api

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/go-rootcerts"
)

const (
	HTTPAddrEnvName = "CONSUL_HTTP_ADDR"

	HTTPTokenEnvName = "CONSUL_HTTP_TOKEN"

	HTTPAuthEnvName = "CONSUL_HTTP_AUTH"

	HTTPSSLEnvName = "CONSUL_HTTP_SSL"

	HTTPCAFile = "CONSUL_CACERT"

	HTTPCAPath = "CONSUL_CAPATH"

	HTTPClientCert = "CONSUL_CLIENT_CERT"

	HTTPClientKey = "CONSUL_CLIENT_KEY"

	HTTPTLSServerName = "CONSUL_TLS_SERVER_NAME"

	HTTPSSLVerifyEnvName = "CONSUL_HTTP_SSL_VERIFY"
)

type QueryOptions struct {
	Datacenter string

	AllowStale bool

	RequireConsistent bool

	WaitIndex uint64

	WaitTime time.Duration

	Token string

	Near string

	NodeMeta map[string]string

	RelayFactor uint8

	ctx context.Context
}

func (o *QueryOptions) Context() context.Context {
	if o != nil && o.ctx != nil {
		return o.ctx
	}
	return context.Background()
}

func (o *QueryOptions) WithContext(ctx context.Context) *QueryOptions {
	o2 := new(QueryOptions)
	if o != nil {
		*o2 = *o
	}
	o2.ctx = ctx
	return o2
}

type WriteOptions struct {
	Datacenter string

	Token string

	RelayFactor uint8

	ctx context.Context
}

func (o *WriteOptions) Context() context.Context {
	if o != nil && o.ctx != nil {
		return o.ctx
	}
	return context.Background()
}

func (o *WriteOptions) WithContext(ctx context.Context) *WriteOptions {
	o2 := new(WriteOptions)
	if o != nil {
		*o2 = *o
	}
	o2.ctx = ctx
	return o2
}

type QueryMeta struct {
	LastIndex uint64

	LastContact time.Duration

	KnownLeader bool

	RequestTime time.Duration

	AddressTranslationEnabled bool
}

type WriteMeta struct {
	RequestTime time.Duration
}

type HttpBasicAuth struct {
	Username string

	Password string
}

type Config struct {
	Address string

	Scheme string

	Datacenter string

	Transport *http.Transport

	HttpClient *http.Client

	HttpAuth *HttpBasicAuth

	WaitTime time.Duration

	Token string

	TLSConfig TLSConfig
}

type TLSConfig struct {
	Address string

	CAFile string

	CAPath string

	CertFile string

	KeyFile string

	InsecureSkipVerify bool
}

// is not recommended, then you may notice idle connections building up over
func DefaultConfig() *Config {
	return defaultConfig(cleanhttp.DefaultPooledTransport)
}

func DefaultNonPooledConfig() *Config {
	return defaultConfig(cleanhttp.DefaultTransport)
}

func defaultConfig(transportFn func() *http.Transport) *Config {
	config := &Config{
		Address:   "127.0.0.1:8500",
		Scheme:    "http",
		Transport: transportFn(),
	}

	if addr := os.Getenv(HTTPAddrEnvName); addr != "" {
		config.Address = addr
	}

	if token := os.Getenv(HTTPTokenEnvName); token != "" {
		config.Token = token
	}

	if auth := os.Getenv(HTTPAuthEnvName); auth != "" {
		var username, password string
		if strings.Contains(auth, ":") {
			split := strings.SplitN(auth, ":", 2)
			username = split[0]
			password = split[1]
		} else {
			username = auth
		}

		config.HttpAuth = &HttpBasicAuth{
			Username: username,
			Password: password,
		}
	}

	if ssl := os.Getenv(HTTPSSLEnvName); ssl != "" {
		enabled, err := strconv.ParseBool(ssl)
		if err != nil {
			log.Printf("[WARN] client: could not parse %s: %s", HTTPSSLEnvName, err)
		}

		if enabled {
			config.Scheme = "https"
		}
	}

	if v := os.Getenv(HTTPTLSServerName); v != "" {
		config.TLSConfig.Address = v
	}
	if v := os.Getenv(HTTPCAFile); v != "" {
		config.TLSConfig.CAFile = v
	}
	if v := os.Getenv(HTTPCAPath); v != "" {
		config.TLSConfig.CAPath = v
	}
	if v := os.Getenv(HTTPClientCert); v != "" {
		config.TLSConfig.CertFile = v
	}
	if v := os.Getenv(HTTPClientKey); v != "" {
		config.TLSConfig.KeyFile = v
	}
	if v := os.Getenv(HTTPSSLVerifyEnvName); v != "" {
		doVerify, err := strconv.ParseBool(v)
		if err != nil {
			log.Printf("[WARN] client: could not parse %s: %s", HTTPSSLVerifyEnvName, err)
		}
		if !doVerify {
			config.TLSConfig.InsecureSkipVerify = true
		}
	}

	return config
}

func SetupTLSConfig(tlsConfig *TLSConfig) (*tls.Config, error) {
	tlsClientConfig := &tls.Config{
		InsecureSkipVerify: tlsConfig.InsecureSkipVerify,
	}

	if tlsConfig.Address != "" {
		server := tlsConfig.Address
		hasPort := strings.LastIndex(server, ":") > strings.LastIndex(server, "]")
		if hasPort {
			var err error
			server, _, err = net.SplitHostPort(server)
			if err != nil {
				return nil, err
			}
		}
		tlsClientConfig.ServerName = server
	}

	if tlsConfig.CertFile != "" && tlsConfig.KeyFile != "" {
		tlsCert, err := tls.LoadX509KeyPair(tlsConfig.CertFile, tlsConfig.KeyFile)
		if err != nil {
			return nil, err
		}
		tlsClientConfig.Certificates = []tls.Certificate{tlsCert}
	}

	if tlsConfig.CAFile != "" || tlsConfig.CAPath != "" {
		rootConfig := &rootcerts.Config{
			CAFile: tlsConfig.CAFile,
			CAPath: tlsConfig.CAPath,
		}
		if err := rootcerts.ConfigureTLS(tlsClientConfig, rootConfig); err != nil {
			return nil, err
		}
	}

	return tlsClientConfig, nil
}

type Client struct {
	config Config
}

func NewClient(config *Config) (*Client, error) {

	defConfig := DefaultConfig()

	if len(config.Address) == 0 {
		config.Address = defConfig.Address
	}

	if len(config.Scheme) == 0 {
		config.Scheme = defConfig.Scheme
	}

	if config.Transport == nil {
		config.Transport = defConfig.Transport
	}

	if config.TLSConfig.Address == "" {
		config.TLSConfig.Address = defConfig.TLSConfig.Address
	}

	if config.TLSConfig.CAFile == "" {
		config.TLSConfig.CAFile = defConfig.TLSConfig.CAFile
	}

	if config.TLSConfig.CAPath == "" {
		config.TLSConfig.CAPath = defConfig.TLSConfig.CAPath
	}

	if config.TLSConfig.CertFile == "" {
		config.TLSConfig.CertFile = defConfig.TLSConfig.CertFile
	}

	if config.TLSConfig.KeyFile == "" {
		config.TLSConfig.KeyFile = defConfig.TLSConfig.KeyFile
	}

	if !config.TLSConfig.InsecureSkipVerify {
		config.TLSConfig.InsecureSkipVerify = defConfig.TLSConfig.InsecureSkipVerify
	}

	if config.HttpClient == nil {
		var err error
		config.HttpClient, err = NewHttpClient(config.Transport, config.TLSConfig)
		if err != nil {
			return nil, err
		}
	}

	parts := strings.SplitN(config.Address, "://", 2)
	if len(parts) == 2 {
		switch parts[0] {
		case "http":
			config.Scheme = "http"
		case "https":
			config.Scheme = "https"
		case "unix":
			trans := cleanhttp.DefaultTransport()
			trans.DialContext = func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", parts[1])
			}
			config.HttpClient = &http.Client{
				Transport: trans,
			}
		default:
			return nil, fmt.Errorf("Unknown protocol scheme: %s", parts[0])
		}
		config.Address = parts[1]
	}

	if config.Token == "" {
		config.Token = defConfig.Token
	}

	return &Client{config: *config}, nil
}

func NewHttpClient(transport *http.Transport, tlsConf TLSConfig) (*http.Client, error) {
	client := &http.Client{
		Transport: transport,
	}

	if transport.TLSClientConfig == nil {
		tlsClientConfig, err := SetupTLSConfig(&tlsConf)

		if err != nil {
			return nil, err
		}

		transport.TLSClientConfig = tlsClientConfig
	}

	return client, nil
}

// request is used to help build up a request
type request struct {
	config *Config
	method string
	url    *url.URL
	params url.Values
	body   io.Reader
	header http.Header
	obj    interface{}
	ctx    context.Context
}

func (r *request) setQueryOptions(q *QueryOptions) {
	if q == nil {
		return
	}
	if q.Datacenter != "" {
		r.params.Set("dc", q.Datacenter)
	}
	if q.AllowStale {
		r.params.Set("stale", "")
	}
	if q.RequireConsistent {
		r.params.Set("consistent", "")
	}
	if q.WaitIndex != 0 {
		r.params.Set("index", strconv.FormatUint(q.WaitIndex, 10))
	}
	if q.WaitTime != 0 {
		r.params.Set("wait", durToMsec(q.WaitTime))
	}
	if q.Token != "" {
		r.header.Set("X-Consul-Token", q.Token)
	}
	if q.Near != "" {
		r.params.Set("near", q.Near)
	}
	if len(q.NodeMeta) > 0 {
		for key, value := range q.NodeMeta {
			r.params.Add("node-meta", key+":"+value)
		}
	}
	if q.RelayFactor != 0 {
		r.params.Set("relay-factor", strconv.Itoa(int(q.RelayFactor)))
	}
	r.ctx = q.ctx
}

func durToMsec(dur time.Duration) string {
	ms := dur / time.Millisecond
	if dur > 0 && ms == 0 {
		ms = 1
	}
	return fmt.Sprintf("%dms", ms)
}

const serverError = "Unexpected response code: 500"

func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	if _, ok := err.(net.Error); ok {
		return true
	}

	return strings.Contains(err.Error(), serverError)
}

func (r *request) setWriteOptions(q *WriteOptions) {
	if q == nil {
		return
	}
	if q.Datacenter != "" {
		r.params.Set("dc", q.Datacenter)
	}
	if q.Token != "" {
		r.header.Set("X-Consul-Token", q.Token)
	}
	if q.RelayFactor != 0 {
		r.params.Set("relay-factor", strconv.Itoa(int(q.RelayFactor)))
	}
	r.ctx = q.ctx
}

func (r *request) toHTTP() (*http.Request, error) {

	r.url.RawQuery = r.params.Encode()

	if r.body == nil && r.obj != nil {
		b, err := encodeBody(r.obj)
		if err != nil {
			return nil, err
		}
		r.body = b
	}

	req, err := http.NewRequest(r.method, r.url.RequestURI(), r.body)
	if err != nil {
		return nil, err
	}

	req.URL.Host = r.url.Host
	req.URL.Scheme = r.url.Scheme
	req.Host = r.url.Host
	req.Header = r.header

	if r.config.HttpAuth != nil {
		req.SetBasicAuth(r.config.HttpAuth.Username, r.config.HttpAuth.Password)
	}
	if r.ctx != nil {
		return req.WithContext(r.ctx), nil
	}

	return req, nil
}

func (c *Client) newRequest(method, path string) *request {
	r := &request{
		config: &c.config,
		method: method,
		url: &url.URL{
			Scheme: c.config.Scheme,
			Host:   c.config.Address,
			Path:   path,
		},
		params: make(map[string][]string),
		header: make(http.Header),
	}
	if c.config.Datacenter != "" {
		r.params.Set("dc", c.config.Datacenter)
	}
	if c.config.WaitTime != 0 {
		r.params.Set("wait", durToMsec(r.config.WaitTime))
	}
	if c.config.Token != "" {
		r.header.Set("X-Consul-Token", r.config.Token)
	}
	return r
}

func (c *Client) doRequest(r *request) (time.Duration, *http.Response, error) {
	req, err := r.toHTTP()
	if err != nil {
		return 0, nil, err
	}
	start := time.Now()
	resp, err := c.config.HttpClient.Do(req)
	diff := time.Since(start)
	return diff, resp, err
}

func (c *Client) query(endpoint string, out interface{}, q *QueryOptions) (*QueryMeta, error) {
	r := c.newRequest("GET", endpoint)
	r.setQueryOptions(q)
	rtt, resp, err := requireOK(c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	qm := &QueryMeta{}
	parseQueryMeta(resp, qm)
	qm.RequestTime = rtt

	if err := decodeBody(resp, out); err != nil {
		return nil, err
	}
	return qm, nil
}

func (c *Client) write(endpoint string, in, out interface{}, q *WriteOptions) (*WriteMeta, error) {
	r := c.newRequest("PUT", endpoint)
	r.setWriteOptions(q)
	r.obj = in
	rtt, resp, err := requireOK(c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	wm := &WriteMeta{RequestTime: rtt}
	if out != nil {
		if err := decodeBody(resp, &out); err != nil {
			return nil, err
		}
	} else if _, err := ioutil.ReadAll(resp.Body); err != nil {
		return nil, err
	}
	return wm, nil
}

func parseQueryMeta(resp *http.Response, q *QueryMeta) error {
	header := resp.Header

	index, err := strconv.ParseUint(header.Get("X-Consul-Index"), 10, 64)
	if err != nil {
		return fmt.Errorf("Failed to parse X-Consul-Index: %v", err)
	}
	q.LastIndex = index

	last, err := strconv.ParseUint(header.Get("X-Consul-LastContact"), 10, 64)
	if err != nil {
		return fmt.Errorf("Failed to parse X-Consul-LastContact: %v", err)
	}
	q.LastContact = time.Duration(last) * time.Millisecond

	switch header.Get("X-Consul-KnownLeader") {
	case "true":
		q.KnownLeader = true
	default:
		q.KnownLeader = false
	}

	switch header.Get("X-Consul-Translate-Addresses") {
	case "true":
		q.AddressTranslationEnabled = true
	default:
		q.AddressTranslationEnabled = false
	}

	return nil
}

func decodeBody(resp *http.Response, out interface{}) error {
	dec := json.NewDecoder(resp.Body)
	return dec.Decode(out)
}

func encodeBody(obj interface{}) (io.Reader, error) {
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(obj); err != nil {
		return nil, err
	}
	return buf, nil
}

func requireOK(d time.Duration, resp *http.Response, e error) (time.Duration, *http.Response, error) {
	if e != nil {
		if resp != nil {
			resp.Body.Close()
		}
		return d, nil, e
	}
	if resp.StatusCode != 200 {
		var buf bytes.Buffer
		io.Copy(&buf, resp.Body)
		resp.Body.Close()
		return d, nil, fmt.Errorf("Unexpected response code: %d (%s)", resp.StatusCode, buf.Bytes())
	}
	return d, resp, nil
}
