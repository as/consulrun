package testutil

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"testing"

	"github.com/pkg/errors"
)

const (
	HealthAny      = "any"
	HealthPassing  = "passing"
	HealthWarning  = "warning"
	HealthCritical = "critical"
	HealthMaint    = "maintenance"
)

func (s *TestServer) JoinLAN(t *testing.T, addr string) {
	resp := s.put(t, "/v1/agent/join/"+addr, nil)
	defer resp.Body.Close()
}

func (s *TestServer) JoinWAN(t *testing.T, addr string) {
	resp := s.put(t, "/v1/agent/join/"+addr+"?wan=1", nil)
	resp.Body.Close()
}

func (s *TestServer) SetKV(t *testing.T, key string, val []byte) {
	resp := s.put(t, "/v1/kv/"+key, bytes.NewBuffer(val))
	resp.Body.Close()
}

func (s *TestServer) SetKVString(t *testing.T, key string, val string) {
	resp := s.put(t, "/v1/kv/"+key, bytes.NewBufferString(val))
	resp.Body.Close()
}

func (s *TestServer) GetKV(t *testing.T, key string) []byte {
	resp := s.get(t, "/v1/kv/"+key)
	defer resp.Body.Close()

	raw, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %s", err)
	}

	var result []*TestKVResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("failed to unmarshal: %s", err)
	}
	if len(result) < 1 {
		t.Fatalf("key does not exist: %s", key)
	}

	v, err := base64.StdEncoding.DecodeString(result[0].Value)
	if err != nil {
		t.Fatalf("failed to base64 decode: %s", err)
	}

	return v
}

func (s *TestServer) GetKVString(t *testing.T, key string) string {
	return string(s.GetKV(t, key))
}

func (s *TestServer) PopulateKV(t *testing.T, data map[string][]byte) {
	for k, v := range data {
		s.SetKV(t, k, v)
	}
}

func (s *TestServer) ListKV(t *testing.T, prefix string) []string {
	resp := s.get(t, "/v1/kv/"+prefix+"?keys")
	defer resp.Body.Close()

	raw, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %s", err)
	}

	var result []string
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatalf("failed to unmarshal: %s", err)
	}
	return result
}

func (s *TestServer) AddService(t *testing.T, name, status string, tags []string) {
	s.AddAddressableService(t, name, status, "", 0, tags) // set empty address and 0 as port for non-accessible service
}

func (s *TestServer) AddAddressableService(t *testing.T, name, status, address string, port int, tags []string) {
	svc := &TestService{
		Name:    name,
		Tags:    tags,
		Address: address,
		Port:    port,
	}
	payload, err := s.encodePayload(svc)
	if err != nil {
		t.Fatal(err)
	}
	s.put(t, "/v1/agent/service/register", payload)

	chkName := "service:" + name
	chk := &TestCheck{
		Name:      chkName,
		ServiceID: name,
		TTL:       "10m",
	}
	payload, err = s.encodePayload(chk)
	if err != nil {
		t.Fatal(err)
	}
	s.put(t, "/v1/agent/check/register", payload)

	switch status {
	case HealthPassing:
		s.put(t, "/v1/agent/check/pass/"+chkName, nil)
	case HealthWarning:
		s.put(t, "/v1/agent/check/warn/"+chkName, nil)
	case HealthCritical:
		s.put(t, "/v1/agent/check/fail/"+chkName, nil)
	default:
		t.Fatalf("Unrecognized status: %s", status)
	}
}

func (s *TestServer) AddCheck(t *testing.T, name, serviceID, status string) {
	chk := &TestCheck{
		ID:   name,
		Name: name,
		TTL:  "10m",
	}
	if serviceID != "" {
		chk.ServiceID = serviceID
	}

	payload, err := s.encodePayload(chk)
	if err != nil {
		t.Fatal(err)
	}
	s.put(t, "/v1/agent/check/register", payload)

	switch status {
	case HealthPassing:
		s.put(t, "/v1/agent/check/pass/"+name, nil)
	case HealthWarning:
		s.put(t, "/v1/agent/check/warn/"+name, nil)
	case HealthCritical:
		s.put(t, "/v1/agent/check/fail/"+name, nil)
	default:
		t.Fatalf("Unrecognized status: %s", status)
	}
}

func (s *TestServer) put(t *testing.T, path string, body io.Reader) *http.Response {
	req, err := http.NewRequest("PUT", s.url(path), body)
	if err != nil {
		t.Fatalf("failed to create PUT request: %s", err)
	}
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		t.Fatalf("failed to make PUT request: %s", err)
	}
	if err := s.requireOK(resp); err != nil {
		defer resp.Body.Close()
		t.Fatalf("not OK PUT: %s", err)
	}
	return resp
}

func (s *TestServer) get(t *testing.T, path string) *http.Response {
	resp, err := s.HTTPClient.Get(s.url(path))
	if err != nil {
		t.Fatalf("failed to create GET request: %s", err)
	}
	if err := s.requireOK(resp); err != nil {
		defer resp.Body.Close()
		t.Fatalf("not OK GET: %s", err)
	}
	return resp
}

func (s *TestServer) encodePayload(payload interface{}) (io.Reader, error) {
	var encoded bytes.Buffer
	enc := json.NewEncoder(&encoded)
	if err := enc.Encode(payload); err != nil {
		return nil, errors.Wrap(err, "failed to encode payload")
	}
	return &encoded, nil
}

func (s *TestServer) url(path string) string {
	if s == nil {
		log.Fatal("s is nil")
	}
	if s.Config == nil {
		log.Fatal("s.Config is nil")
	}
	if s.Config.Ports == nil {
		log.Fatal("s.Config.Ports is nil")
	}
	if s.Config.Ports.HTTP == 0 {
		log.Fatal("s.Config.Ports.HTTP is 0")
	}
	if path == "" {
		log.Fatal("path is empty")
	}
	return fmt.Sprintf("http://127.0.0.1:%d%s", s.Config.Ports.HTTP, path)
}

func (s *TestServer) requireOK(resp *http.Response) error {
	if resp.StatusCode != 200 {
		return fmt.Errorf("Bad status code: %d", resp.StatusCode)
	}
	return nil
}
