package testutil

//

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/as/consulrun/hashicorp/consul/lib/freeport"
	"github.com/as/consulrun/hashicorp/consul/testutil/retry"
	"github.com/as/consulrun/hashicorp/go-cleanhttp"
	"github.com/as/consulrun/hashicorp/go-uuid"
	"github.com/pkg/errors"
)

type TestPerformanceConfig struct {
	RaftMultiplier uint `json:"raft_multiplier,omitempty"`
}

type TestPortConfig struct {
	DNS     int `json:"dns,omitempty"`
	HTTP    int `json:"http,omitempty"`
	HTTPS   int `json:"https,omitempty"`
	SerfLan int `json:"serf_lan,omitempty"`
	SerfWan int `json:"serf_wan,omitempty"`
	Server  int `json:"server,omitempty"`
}

type TestAddressConfig struct {
	HTTP string `json:"http,omitempty"`
}

type TestNetworkSegment struct {
	Name      string `json:"name"`
	Bind      string `json:"bind"`
	Port      int    `json:"port"`
	Advertise string `json:"advertise"`
}

type TestServerConfig struct {
	NodeName            string                 `json:"node_name"`
	NodeID              string                 `json:"node_id"`
	NodeMeta            map[string]string      `json:"node_meta,omitempty"`
	Performance         *TestPerformanceConfig `json:"performance,omitempty"`
	Bootstrap           bool                   `json:"bootstrap,omitempty"`
	Server              bool                   `json:"server,omitempty"`
	DataDir             string                 `json:"data_dir,omitempty"`
	Datacenter          string                 `json:"datacenter,omitempty"`
	Segments            []TestNetworkSegment   `json:"segments"`
	DisableCheckpoint   bool                   `json:"disable_update_check"`
	LogLevel            string                 `json:"log_level,omitempty"`
	Bind                string                 `json:"bind_addr,omitempty"`
	Addresses           *TestAddressConfig     `json:"addresses,omitempty"`
	Ports               *TestPortConfig        `json:"ports,omitempty"`
	RaftProtocol        int                    `json:"raft_protocol,omitempty"`
	ACLMasterToken      string                 `json:"acl_master_token,omitempty"`
	ACLDatacenter       string                 `json:"acl_datacenter,omitempty"`
	ACLDefaultPolicy    string                 `json:"acl_default_policy,omitempty"`
	ACLEnforceVersion8  bool                   `json:"acl_enforce_version_8"`
	Encrypt             string                 `json:"encrypt,omitempty"`
	CAFile              string                 `json:"ca_file,omitempty"`
	CertFile            string                 `json:"cert_file,omitempty"`
	KeyFile             string                 `json:"key_file,omitempty"`
	VerifyIncoming      bool                   `json:"verify_incoming,omitempty"`
	VerifyIncomingRPC   bool                   `json:"verify_incoming_rpc,omitempty"`
	VerifyIncomingHTTPS bool                   `json:"verify_incoming_https,omitempty"`
	VerifyOutgoing      bool                   `json:"verify_outgoing,omitempty"`
	EnableScriptChecks  bool                   `json:"enable_script_checks,omitempty"`
	ReadyTimeout        time.Duration          `json:"-"`
	Stdout, Stderr      io.Writer              `json:"-"`
	Args                []string               `json:"-"`
}

type ServerConfigCallback func(c *TestServerConfig)

func defaultServerConfig() *TestServerConfig {
	nodeID, err := uuid.GenerateUUID()
	if err != nil {
		panic(err)
	}

	ports := freeport.Get(6)
	return &TestServerConfig{
		NodeName:          "node-" + nodeID,
		NodeID:            nodeID,
		DisableCheckpoint: true,
		Performance: &TestPerformanceConfig{
			RaftMultiplier: 1,
		},
		Bootstrap: true,
		Server:    true,
		LogLevel:  "debug",
		Bind:      "127.0.0.1",
		Addresses: &TestAddressConfig{},
		Ports: &TestPortConfig{
			DNS:     ports[0],
			HTTP:    ports[1],
			HTTPS:   ports[2],
			SerfLan: ports[3],
			SerfWan: ports[4],
			Server:  ports[5],
		},
		ReadyTimeout: 10 * time.Second,
	}
}

type TestService struct {
	ID      string   `json:",omitempty"`
	Name    string   `json:",omitempty"`
	Tags    []string `json:",omitempty"`
	Address string   `json:",omitempty"`
	Port    int      `json:",omitempty"`
}

type TestCheck struct {
	ID        string `json:",omitempty"`
	Name      string `json:",omitempty"`
	ServiceID string `json:",omitempty"`
	TTL       string `json:",omitempty"`
}

type TestKVResponse struct {
	Value string
}

type TestServer struct {
	cmd    *exec.Cmd
	Config *TestServerConfig

	HTTPAddr  string
	HTTPSAddr string
	LANAddr   string
	WANAddr   string

	HTTPClient *http.Client

	tmpdir string
}

func NewTestServer() (*TestServer, error) {
	return NewTestServerConfigT(nil, nil)
}

func NewTestServerConfig(cb ServerConfigCallback) (*TestServer, error) {
	return NewTestServerConfigT(nil, cb)
}

func NewTestServerConfigT(t *testing.T, cb ServerConfigCallback) (*TestServer, error) {
	return newTestServerConfigT(t, cb)
}

func newTestServerConfigT(t *testing.T, cb ServerConfigCallback) (*TestServer, error) {
	path, err := exec.LookPath("consul")
	if err != nil || path == "" {
		return nil, fmt.Errorf("consul not found on $PATH - download and install " +
			"consul or skip this test")
	}

	tmpdir := TempDir(t, "consul")
	cfg := defaultServerConfig()
	cfg.DataDir = filepath.Join(tmpdir, "data")
	if cb != nil {
		cb(cfg)
	}

	b, err := json.Marshal(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed marshaling json")
	}

	configFile := filepath.Join(tmpdir, "config.json")
	if err := ioutil.WriteFile(configFile, b, 0644); err != nil {
		defer os.RemoveAll(tmpdir)
		return nil, errors.Wrap(err, "failed writing config content")
	}

	stdout := io.Writer(os.Stdout)
	if cfg.Stdout != nil {
		stdout = cfg.Stdout
	}
	stderr := io.Writer(os.Stderr)
	if cfg.Stderr != nil {
		stderr = cfg.Stderr
	}

	args := []string{"agent", "-config-file", configFile}
	args = append(args, cfg.Args...)
	cmd := exec.Command("consul", args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return nil, errors.Wrap(err, "failed starting command")
	}

	httpAddr := fmt.Sprintf("127.0.0.1:%d", cfg.Ports.HTTP)
	client := cleanhttp.DefaultClient()
	if strings.HasPrefix(cfg.Addresses.HTTP, "unix://") {
		httpAddr = cfg.Addresses.HTTP
		tr := cleanhttp.DefaultTransport()
		tr.DialContext = func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", httpAddr[len("unix://"):])
		}
		client = &http.Client{Transport: tr}
	}

	server := &TestServer{
		Config: cfg,
		cmd:    cmd,

		HTTPAddr:  httpAddr,
		HTTPSAddr: fmt.Sprintf("127.0.0.1:%d", cfg.Ports.HTTPS),
		LANAddr:   fmt.Sprintf("127.0.0.1:%d", cfg.Ports.SerfLan),
		WANAddr:   fmt.Sprintf("127.0.0.1:%d", cfg.Ports.SerfWan),

		HTTPClient: client,

		tmpdir: tmpdir,
	}

	if cfg.Bootstrap {
		err = server.waitForLeader()
	} else {
		err = server.waitForAPI()
	}
	if err != nil {
		defer server.Stop()
		return nil, errors.Wrap(err, "failed waiting for server to start")
	}
	return server, nil
}

func (s *TestServer) Stop() error {
	defer os.RemoveAll(s.tmpdir)

	if s.cmd == nil {
		return nil
	}

	if s.cmd.Process != nil {
		if err := s.cmd.Process.Signal(os.Interrupt); err != nil {
			return errors.Wrap(err, "failed to kill consul server")
		}
	}

	return s.cmd.Wait()
}

type failer struct {
	failed bool
}

func (f *failer) Log(args ...interface{}) { fmt.Println(args) }
func (f *failer) FailNow()                { f.failed = true }

func (s *TestServer) waitForAPI() error {
	f := &failer{}
	retry.Run(f, func(r *retry.R) {
		resp, err := s.HTTPClient.Get(s.url("/v1/agent/self"))
		if err != nil {
			r.Fatal(err)
		}
		defer resp.Body.Close()
		if err := s.requireOK(resp); err != nil {
			r.Fatal("failed OK response", err)
		}
	})
	if f.failed {
		return errors.New("failed waiting for API")
	}
	return nil
}

func (s *TestServer) waitForLeader() error {
	f := &failer{}
	timer := &retry.Timer{
		Timeout: s.Config.ReadyTimeout,
		Wait:    250 * time.Millisecond,
	}
	var index int64
	retry.RunWith(timer, f, func(r *retry.R) {

		url := s.url(fmt.Sprintf("/v1/catalog/nodes?index=%d", index))
		resp, err := s.HTTPClient.Get(url)
		if err != nil {
			r.Fatal("failed http get", err)
		}
		defer resp.Body.Close()
		if err := s.requireOK(resp); err != nil {
			r.Fatal("failed OK response", err)
		}

		if leader := resp.Header.Get("X-Consul-KnownLeader"); leader != "true" {
			r.Fatalf("Consul leader status: %#v", leader)
		}
		index, err = strconv.ParseInt(resp.Header.Get("X-Consul-Index"), 10, 64)
		if err != nil {
			r.Fatal("bad consul index", err)
		}
		if index == 0 {
			r.Fatal("consul index is 0")
		}

		var v []map[string]interface{}
		dec := json.NewDecoder(resp.Body)
		if err := dec.Decode(&v); err != nil {
			r.Fatal(err)
		}
		if len(v) < 1 {
			r.Fatal("No nodes")
		}
		taggedAddresses, ok := v[0]["TaggedAddresses"].(map[string]interface{})
		if !ok {
			r.Fatal("Missing tagged addresses")
		}
		if _, ok := taggedAddresses["lan"]; !ok {
			r.Fatal("No lan tagged addresses")
		}
	})
	if f.failed {
		return errors.New("failed waiting for leader")
	}
	return nil
}
