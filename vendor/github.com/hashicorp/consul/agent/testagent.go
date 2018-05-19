package agent

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/consul/agent/config"
	"github.com/hashicorp/consul/agent/consul"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/lib/freeport"
	"github.com/hashicorp/consul/logger"
	"github.com/hashicorp/consul/testutil/retry"
	uuid "github.com/hashicorp/go-uuid"
)

func init() {
	rand.Seed(time.Now().UnixNano()) // seed random number generator
}

var TempDir = os.TempDir()

type TestAgent struct {
	Name string

	HCL string

	Config *config.RuntimeConfig

	LogOutput io.Writer

	LogWriter *logger.LogWriter

	DataDir string

	Key string

	UseTLS bool

	dns *DNSServer

	srv *HTTPServer

	*Agent
}

func NewTestAgent(name string, hcl string) *TestAgent {
	a := &TestAgent{Name: name, HCL: hcl}
	a.Start()
	return a
}

type panicFailer struct{}

func (f *panicFailer) Log(args ...interface{}) { fmt.Println(args...) }
func (f *panicFailer) FailNow()                { panic("failed") }

func (a *TestAgent) Start() *TestAgent {
	if a.Agent != nil {
		panic("TestAgent already started")
	}
	var hclDataDir string
	if a.DataDir == "" {
		name := "agent"
		if a.Name != "" {
			name = a.Name + "-agent"
		}
		name = strings.Replace(name, "/", "_", -1)
		d, err := ioutil.TempDir(TempDir, name)
		if err != nil {
			panic(fmt.Sprintf("Error creating data dir %s: %s", filepath.Join(TempDir, name), err))
		}
		hclDataDir = `data_dir = "` + d + `"`
	}
	id := NodeID()

	for i := 10; i >= 0; i-- {
		a.Config = TestConfig(
			config.Source{Name: a.Name, Format: "hcl", Data: a.HCL},
			config.Source{Name: a.Name + ".data_dir", Format: "hcl", Data: hclDataDir},
			randomPortsSource(a.UseTLS),
		)

		if a.Key != "" {
			writeKey := func(key, filename string) {
				path := filepath.Join(a.Config.DataDir, filename)
				if err := initKeyring(path, key); err != nil {
					panic(fmt.Sprintf("Error creating keyring %s: %s", path, err))
				}
			}
			writeKey(a.Key, SerfLANKeyring)
			writeKey(a.Key, SerfWANKeyring)
		}

		agent, err := New(a.Config)
		if err != nil {
			panic(fmt.Sprintf("Error creating agent: %s", err))
		}

		logOutput := a.LogOutput
		if logOutput == nil {
			logOutput = os.Stderr
		}
		agent.LogOutput = logOutput
		agent.LogWriter = a.LogWriter
		agent.logger = log.New(logOutput, a.Name+" - ", log.LstdFlags|log.Lmicroseconds)
		agent.MemSink = metrics.NewInmemSink(1*time.Second, time.Minute)

		if err := agent.Start(); err == nil {
			a.Agent = agent
			break
		} else if i == 0 {
			fmt.Println(id, a.Name, "Error starting agent:", err)
			runtime.Goexit()
		} else {
			agent.ShutdownAgent()
			agent.ShutdownEndpoints()
			wait := time.Duration(rand.Int31n(2000)) * time.Millisecond
			fmt.Println(id, a.Name, "retrying in", wait)
			time.Sleep(wait)
		}

		if a.DataDir != "" {
			if err := os.RemoveAll(a.DataDir); err != nil {
				fmt.Println(id, a.Name, "Error resetting data dir:", err)
				runtime.Goexit()
			}
		}
	}

	a.Agent.StartSync()

	var out structs.IndexedNodes
	retry.Run(&panicFailer{}, func(r *retry.R) {
		if len(a.httpServers) == 0 {
			r.Fatal(a.Name, "waiting for server")
		}
		if a.Config.Bootstrap && a.Config.ServerMode {

			args := &structs.DCSpecificRequest{
				Datacenter: a.Config.Datacenter,
				QueryOptions: structs.QueryOptions{
					MinQueryIndex: out.Index,
					MaxQueryTime:  25 * time.Millisecond,
				},
			}
			if err := a.RPC("Catalog.ListNodes", args, &out); err != nil {
				r.Fatal(a.Name, "Catalog.ListNodes failed:", err)
			}
			if !out.QueryMeta.KnownLeader {
				r.Fatal(a.Name, "No leader")
			}
			if out.Index == 0 {
				r.Fatal(a.Name, ": Consul index is 0")
			}
		} else {
			req, _ := http.NewRequest("GET", "/v1/agent/self", nil)
			resp := httptest.NewRecorder()
			_, err := a.httpServers[0].AgentSelf(resp, req)
			if err != nil || resp.Code != 200 {
				r.Fatal(a.Name, "failed OK response", err)
			}
		}
	})
	a.dns = a.dnsServers[0]
	a.srv = a.httpServers[0]
	return a
}

func (a *TestAgent) Shutdown() error {
	/* Removed this because it was breaking persistence tests where we would
	persist a service and load it through a new agent with the same data-dir.
	Not sure if we still need this for other things, everywhere we manually make
	a data dir we already do 'defer os.RemoveAll()'
	defer func() {
		if a.DataDir != "" {
			os.RemoveAll(a.DataDir)
		}
	}()*/

	defer a.Agent.ShutdownEndpoints()
	return a.Agent.ShutdownAgent()
}

func (a *TestAgent) DNSAddr() string {
	if a.dns == nil {
		return ""
	}
	return a.dns.Addr
}

func (a *TestAgent) HTTPAddr() string {
	if a.srv == nil {
		return ""
	}
	return a.srv.Addr
}

func (a *TestAgent) SegmentAddr(name string) string {
	if server, ok := a.Agent.delegate.(*consul.Server); ok {
		return server.LANSegmentAddr(name)
	}
	return ""
}

func (a *TestAgent) Client() *api.Client {
	conf := api.DefaultConfig()
	conf.Address = a.HTTPAddr()
	c, err := api.NewClient(conf)
	if err != nil {
		panic(fmt.Sprintf("Error creating consul API client: %s", err))
	}
	return c
}

func (a *TestAgent) DNSDisableCompression(b bool) {
	for _, srv := range a.dnsServers {
		srv.disableCompression.Store(b)
	}
}

func (a *TestAgent) consulConfig() *consul.Config {
	c, err := a.Agent.consulConfig()
	if err != nil {
		panic(err)
	}
	return c
}

func randomPortsSource(tls bool) config.Source {
	ports := freeport.Get(6)
	if tls {
		ports[1] = -1
	} else {
		ports[2] = -1
	}
	return config.Source{
		Name:   "ports",
		Format: "hcl",
		Data: `
			ports = {
				dns = ` + strconv.Itoa(ports[0]) + `
				http = ` + strconv.Itoa(ports[1]) + `
				https = ` + strconv.Itoa(ports[2]) + `
				serf_lan = ` + strconv.Itoa(ports[3]) + `
				serf_wan = ` + strconv.Itoa(ports[4]) + `
				server = ` + strconv.Itoa(ports[5]) + `
			}
		`,
	}
}

func NodeID() string {
	id, err := uuid.GenerateUUID()
	if err != nil {
		panic(err)
	}
	return id
}

func TestConfig(sources ...config.Source) *config.RuntimeConfig {
	nodeID := NodeID()
	testsrc := config.Source{
		Name:   "test",
		Format: "hcl",
		Data: `
			bind_addr = "127.0.0.1"
			advertise_addr = "127.0.0.1"
			datacenter = "dc1"
			bootstrap = true
			server = true
			node_id = "` + nodeID + `"
			node_name = "Node ` + nodeID + `"
			performance {
				raft_multiplier = 1
			}
		`,
	}

	b, err := config.NewBuilder(config.Flags{})
	if err != nil {
		panic("NewBuilder failed: " + err.Error())
	}
	b.Head = append(b.Head, testsrc)
	b.Tail = append(b.Tail, config.DefaultConsulSource(), config.DevConsulSource())
	b.Tail = append(b.Tail, sources...)

	cfg, err := b.BuildAndValidate()
	if err != nil {
		panic("Error building config: " + err.Error())
	}

	for _, w := range b.Warnings {
		fmt.Println("WARNING:", w)
	}

	return &cfg
}

func TestACLConfig() string {
	return `
		acl_datacenter = "dc1"
		acl_default_policy = "deny"
		acl_master_token = "root"
		acl_agent_token = "root"
		acl_agent_master_token = "towel"
		acl_enforce_version_8 = true
	`
}
