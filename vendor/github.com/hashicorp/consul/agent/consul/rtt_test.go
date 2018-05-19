package consul

import (
	"fmt"
	"net/rpc"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/lib"
	"github.com/hashicorp/consul/testrpc"
	"github.com/hashicorp/net-rpc-msgpackrpc"
)

func verifyNodeSort(t *testing.T, nodes structs.Nodes, expected string) {
	vec := make([]string, len(nodes))
	for i, node := range nodes {
		vec[i] = node.Node
	}
	actual := strings.Join(vec, ",")
	if actual != expected {
		t.Fatalf("bad sort: %s != %s", actual, expected)
	}
}

func verifyServiceNodeSort(t *testing.T, nodes structs.ServiceNodes, expected string) {
	vec := make([]string, len(nodes))
	for i, node := range nodes {
		vec[i] = node.Node
	}
	actual := strings.Join(vec, ",")
	if actual != expected {
		t.Fatalf("bad sort: %s != %s", actual, expected)
	}
}

func verifyHealthCheckSort(t *testing.T, checks structs.HealthChecks, expected string) {
	vec := make([]string, len(checks))
	for i, check := range checks {
		vec[i] = check.Node
	}
	actual := strings.Join(vec, ",")
	if actual != expected {
		t.Fatalf("bad sort: %s != %s", actual, expected)
	}
}

func verifyCheckServiceNodeSort(t *testing.T, nodes structs.CheckServiceNodes, expected string) {
	vec := make([]string, len(nodes))
	for i, node := range nodes {
		vec[i] = node.Node.Node
	}
	actual := strings.Join(vec, ",")
	if actual != expected {
		t.Fatalf("bad sort: %s != %s", actual, expected)
	}
}

//
//
//
func seedCoordinates(t *testing.T, codec rpc.ClientCodec, server *Server) {

	for i := 0; i < 5; i++ {
		req := structs.RegisterRequest{
			Datacenter: "dc1",
			Node:       fmt.Sprintf("node%d", i+1),
			Address:    "127.0.0.1",
		}
		var reply struct{}
		if err := msgpackrpc.CallWithCodec(codec, "Catalog.Register", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	updates := []structs.CoordinateUpdateRequest{
		{
			Datacenter: "dc1",
			Node:       "node1",
			Coord:      lib.GenerateCoordinate(10 * time.Millisecond),
		},
		{
			Datacenter: "dc1",
			Node:       "node2",
			Coord:      lib.GenerateCoordinate(2 * time.Millisecond),
		},
		{
			Datacenter: "dc1",
			Node:       "node3",
			Coord:      lib.GenerateCoordinate(1 * time.Millisecond),
		},
		{
			Datacenter: "dc1",
			Node:       "node4",
			Coord:      lib.GenerateCoordinate(8 * time.Millisecond),
		},
		{
			Datacenter: "dc1",
			Node:       "node5",
			Coord:      lib.GenerateCoordinate(3 * time.Millisecond),
		},
	}

	for _, update := range updates {
		var out struct{}
		if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &update, &out); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	time.Sleep(2 * server.config.CoordinateUpdatePeriod)
}

func TestRTT_sortNodesByDistanceFrom(t *testing.T) {
	t.Parallel()
	dir, server := testServer(t)
	defer os.RemoveAll(dir)
	defer server.Shutdown()

	codec := rpcClient(t, server)
	defer codec.Close()
	testrpc.WaitForLeader(t, server.RPC, "dc1")

	seedCoordinates(t, codec, server)
	nodes := structs.Nodes{
		&structs.Node{Node: "apple"},
		&structs.Node{Node: "node1"},
		&structs.Node{Node: "node2"},
		&structs.Node{Node: "node3"},
		&structs.Node{Node: "node4"},
		&structs.Node{Node: "node5"},
	}

	var source structs.QuerySource
	if err := server.sortNodesByDistanceFrom(source, nodes); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyNodeSort(t, nodes, "apple,node1,node2,node3,node4,node5")

	source.Node = "node1"
	source.Datacenter = "dc2"
	if err := server.sortNodesByDistanceFrom(source, nodes); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyNodeSort(t, nodes, "apple,node1,node2,node3,node4,node5")

	source.Node = "apple"
	source.Datacenter = "dc1"
	if err := server.sortNodesByDistanceFrom(source, nodes); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyNodeSort(t, nodes, "apple,node1,node2,node3,node4,node5")

	source.Node = "node1"
	if err := server.sortNodesByDistanceFrom(source, nodes); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyNodeSort(t, nodes, "node1,node4,node5,node2,node3,apple")
}

func TestRTT_sortNodesByDistanceFrom_Nodes(t *testing.T) {
	t.Parallel()
	dir, server := testServer(t)
	defer os.RemoveAll(dir)
	defer server.Shutdown()

	codec := rpcClient(t, server)
	defer codec.Close()
	testrpc.WaitForLeader(t, server.RPC, "dc1")

	seedCoordinates(t, codec, server)
	nodes := structs.Nodes{
		&structs.Node{Node: "apple"},
		&structs.Node{Node: "node1"},
		&structs.Node{Node: "node2"},
		&structs.Node{Node: "node3"},
		&structs.Node{Node: "node4"},
		&structs.Node{Node: "node5"},
	}

	var source structs.QuerySource
	source.Node = "node1"
	source.Datacenter = "dc1"
	if err := server.sortNodesByDistanceFrom(source, nodes); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyNodeSort(t, nodes, "node1,node4,node5,node2,node3,apple")

	source.Node = "node2"
	source.Datacenter = "dc1"
	if err := server.sortNodesByDistanceFrom(source, nodes); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyNodeSort(t, nodes, "node2,node5,node3,node4,node1,apple")

	nodes[1], nodes[2] = nodes[2], nodes[1]
	if err := server.sortNodesByDistanceFrom(source, nodes); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyNodeSort(t, nodes, "node2,node3,node5,node4,node1,apple")
}

func TestRTT_sortNodesByDistanceFrom_ServiceNodes(t *testing.T) {
	t.Parallel()
	dir, server := testServer(t)
	defer os.RemoveAll(dir)
	defer server.Shutdown()

	codec := rpcClient(t, server)
	defer codec.Close()
	testrpc.WaitForLeader(t, server.RPC, "dc1")

	seedCoordinates(t, codec, server)
	nodes := structs.ServiceNodes{
		&structs.ServiceNode{Node: "apple"},
		&structs.ServiceNode{Node: "node1"},
		&structs.ServiceNode{Node: "node2"},
		&structs.ServiceNode{Node: "node3"},
		&structs.ServiceNode{Node: "node4"},
		&structs.ServiceNode{Node: "node5"},
	}

	var source structs.QuerySource
	source.Node = "node1"
	source.Datacenter = "dc1"
	if err := server.sortNodesByDistanceFrom(source, nodes); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyServiceNodeSort(t, nodes, "node1,node4,node5,node2,node3,apple")

	source.Node = "node2"
	source.Datacenter = "dc1"
	if err := server.sortNodesByDistanceFrom(source, nodes); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyServiceNodeSort(t, nodes, "node2,node5,node3,node4,node1,apple")

	nodes[1], nodes[2] = nodes[2], nodes[1]
	if err := server.sortNodesByDistanceFrom(source, nodes); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyServiceNodeSort(t, nodes, "node2,node3,node5,node4,node1,apple")
}

func TestRTT_sortNodesByDistanceFrom_HealthChecks(t *testing.T) {
	t.Parallel()
	dir, server := testServer(t)
	defer os.RemoveAll(dir)
	defer server.Shutdown()

	codec := rpcClient(t, server)
	defer codec.Close()
	testrpc.WaitForLeader(t, server.RPC, "dc1")

	seedCoordinates(t, codec, server)
	checks := structs.HealthChecks{
		&structs.HealthCheck{Node: "apple"},
		&structs.HealthCheck{Node: "node1"},
		&structs.HealthCheck{Node: "node2"},
		&structs.HealthCheck{Node: "node3"},
		&structs.HealthCheck{Node: "node4"},
		&structs.HealthCheck{Node: "node5"},
	}

	var source structs.QuerySource
	source.Node = "node1"
	source.Datacenter = "dc1"
	if err := server.sortNodesByDistanceFrom(source, checks); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyHealthCheckSort(t, checks, "node1,node4,node5,node2,node3,apple")

	source.Node = "node2"
	source.Datacenter = "dc1"
	if err := server.sortNodesByDistanceFrom(source, checks); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyHealthCheckSort(t, checks, "node2,node5,node3,node4,node1,apple")

	checks[1], checks[2] = checks[2], checks[1]
	if err := server.sortNodesByDistanceFrom(source, checks); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyHealthCheckSort(t, checks, "node2,node3,node5,node4,node1,apple")
}

func TestRTT_sortNodesByDistanceFrom_CheckServiceNodes(t *testing.T) {
	t.Parallel()
	dir, server := testServer(t)
	defer os.RemoveAll(dir)
	defer server.Shutdown()

	codec := rpcClient(t, server)
	defer codec.Close()
	testrpc.WaitForLeader(t, server.RPC, "dc1")

	seedCoordinates(t, codec, server)
	nodes := structs.CheckServiceNodes{
		structs.CheckServiceNode{Node: &structs.Node{Node: "apple"}},
		structs.CheckServiceNode{Node: &structs.Node{Node: "node1"}},
		structs.CheckServiceNode{Node: &structs.Node{Node: "node2"}},
		structs.CheckServiceNode{Node: &structs.Node{Node: "node3"}},
		structs.CheckServiceNode{Node: &structs.Node{Node: "node4"}},
		structs.CheckServiceNode{Node: &structs.Node{Node: "node5"}},
	}

	var source structs.QuerySource
	source.Node = "node1"
	source.Datacenter = "dc1"
	if err := server.sortNodesByDistanceFrom(source, nodes); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyCheckServiceNodeSort(t, nodes, "node1,node4,node5,node2,node3,apple")

	source.Node = "node2"
	source.Datacenter = "dc1"
	if err := server.sortNodesByDistanceFrom(source, nodes); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyCheckServiceNodeSort(t, nodes, "node2,node5,node3,node4,node1,apple")

	nodes[1], nodes[2] = nodes[2], nodes[1]
	if err := server.sortNodesByDistanceFrom(source, nodes); err != nil {
		t.Fatalf("err: %v", err)
	}
	verifyCheckServiceNodeSort(t, nodes, "node2,node3,node5,node4,node1,apple")
}
