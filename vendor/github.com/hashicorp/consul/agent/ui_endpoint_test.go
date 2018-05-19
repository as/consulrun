package agent

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/testutil"
	cleanhttp "github.com/hashicorp/go-cleanhttp"
)

func TestUiIndex(t *testing.T) {
	t.Parallel()

	uiDir := testutil.TempDir(t, "consul")
	defer os.RemoveAll(uiDir)

	a := NewTestAgent(t.Name(), `
		ui_dir = "`+uiDir+`"
	`)
	defer a.Shutdown()

	path := filepath.Join(a.Config.UIDir, "my-file")
	if err := ioutil.WriteFile(path, []byte("test"), 777); err != nil {
		t.Fatalf("err: %v", err)
	}

	req, _ := http.NewRequest("GET", "/ui/my-file", nil)
	req.URL.Scheme = "http"
	req.URL.Host = a.srv.Addr

	client := cleanhttp.DefaultClient()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if resp.StatusCode != 200 {
		t.Fatalf("bad: %v", resp)
	}

	out := bytes.NewBuffer(nil)
	io.Copy(out, resp.Body)
	if string(out.Bytes()) != "test" {
		t.Fatalf("bad: %s", out.Bytes())
	}
}

func TestUiNodes(t *testing.T) {
	t.Parallel()
	a := NewTestAgent(t.Name(), "")
	defer a.Shutdown()

	args := &structs.RegisterRequest{
		Datacenter: "dc1",
		Node:       "test",
		Address:    "127.0.0.1",
	}

	var out struct{}
	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	req, _ := http.NewRequest("GET", "/v1/internal/ui/nodes/dc1", nil)
	resp := httptest.NewRecorder()
	obj, err := a.srv.UINodes(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	assertIndex(t, resp)

	nodes := obj.(structs.NodeDump)
	if len(nodes) != 2 ||
		nodes[0].Node != a.Config.NodeName ||
		nodes[0].Services == nil || len(nodes[0].Services) != 1 ||
		nodes[0].Checks == nil || len(nodes[0].Checks) != 1 ||
		nodes[1].Node != "test" ||
		nodes[1].Services == nil || len(nodes[1].Services) != 0 ||
		nodes[1].Checks == nil || len(nodes[1].Checks) != 0 {
		t.Fatalf("bad: %v", obj)
	}
}

func TestUiNodeInfo(t *testing.T) {
	t.Parallel()
	a := NewTestAgent(t.Name(), "")
	defer a.Shutdown()

	req, _ := http.NewRequest("GET", fmt.Sprintf("/v1/internal/ui/node/%s", a.Config.NodeName), nil)
	resp := httptest.NewRecorder()
	obj, err := a.srv.UINodeInfo(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	assertIndex(t, resp)

	node := obj.(*structs.NodeInfo)
	if node.Node != a.Config.NodeName {
		t.Fatalf("bad: %v", node)
	}

	args := &structs.RegisterRequest{
		Datacenter: "dc1",
		Node:       "test",
		Address:    "127.0.0.1",
	}

	var out struct{}
	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	req, _ = http.NewRequest("GET", "/v1/internal/ui/node/test", nil)
	resp = httptest.NewRecorder()
	obj, err = a.srv.UINodeInfo(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	assertIndex(t, resp)

	node = obj.(*structs.NodeInfo)
	if node.Node != "test" ||
		node.Services == nil || len(node.Services) != 0 ||
		node.Checks == nil || len(node.Checks) != 0 {
		t.Fatalf("bad: %v", node)
	}
}

func TestSummarizeServices(t *testing.T) {
	t.Parallel()
	dump := structs.NodeDump{
		&structs.NodeInfo{
			Node:    "foo",
			Address: "127.0.0.1",
			Services: []*structs.NodeService{
				{
					Service: "api",
					Tags:    []string{"tag1", "tag2"},
				},
				{
					Service: "web",
					Tags:    []string{},
				},
			},
			Checks: []*structs.HealthCheck{
				{
					Status:      api.HealthPassing,
					ServiceName: "",
				},
				{
					Status:      api.HealthPassing,
					ServiceName: "web",
				},
				{
					Status:      api.HealthWarning,
					ServiceName: "api",
				},
			},
		},
		&structs.NodeInfo{
			Node:    "bar",
			Address: "127.0.0.2",
			Services: []*structs.NodeService{
				{
					Service: "web",
					Tags:    []string{},
				},
			},
			Checks: []*structs.HealthCheck{
				{
					Status:      api.HealthCritical,
					ServiceName: "web",
				},
			},
		},
		&structs.NodeInfo{
			Node:    "zip",
			Address: "127.0.0.3",
			Services: []*structs.NodeService{
				{
					Service: "cache",
					Tags:    []string{},
				},
			},
		},
	}

	summary := summarizeServices(dump)
	if len(summary) != 3 {
		t.Fatalf("bad: %v", summary)
	}

	expectAPI := &ServiceSummary{
		Name:           "api",
		Tags:           []string{"tag1", "tag2"},
		Nodes:          []string{"foo"},
		ChecksPassing:  1,
		ChecksWarning:  1,
		ChecksCritical: 0,
	}
	if !reflect.DeepEqual(summary[0], expectAPI) {
		t.Fatalf("bad: %v", summary[0])
	}

	expectCache := &ServiceSummary{
		Name:           "cache",
		Tags:           []string{},
		Nodes:          []string{"zip"},
		ChecksPassing:  0,
		ChecksWarning:  0,
		ChecksCritical: 0,
	}
	if !reflect.DeepEqual(summary[1], expectCache) {
		t.Fatalf("bad: %v", summary[1])
	}

	expectWeb := &ServiceSummary{
		Name:           "web",
		Tags:           []string{},
		Nodes:          []string{"bar", "foo"},
		ChecksPassing:  2,
		ChecksWarning:  0,
		ChecksCritical: 1,
	}
	if !reflect.DeepEqual(summary[2], expectWeb) {
		t.Fatalf("bad: %v", summary[2])
	}
}
