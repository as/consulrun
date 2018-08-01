package consul

import (
	"fmt"
	"math"
	"math/rand"
	"net/rpc"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/as/consulrun/hashicorp/consul/acl"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/lib"
	"github.com/as/consulrun/hashicorp/consul/testrpc"
	"github.com/as/consulrun/hashicorp/consul/testutil/retry"
	"github.com/as/consulrun/hashicorp/net-rpc-msgpackrpc"
	"github.com/as/consulrun/hashicorp/serf/coordinate"
	"github.com/pascaldekloe/goe/verify"
)

func generateRandomCoordinate() *coordinate.Coordinate {
	config := coordinate.DefaultConfig()
	coord := coordinate.NewCoordinate(config)
	for i := range coord.Vec {
		coord.Vec[i] = rand.NormFloat64()
	}
	coord.Error = rand.NormFloat64()
	coord.Adjustment = rand.NormFloat64()
	return coord
}

func TestCoordinate_Update(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.CoordinateUpdatePeriod = 500 * time.Millisecond
		c.CoordinateUpdateBatchSize = 5
		c.CoordinateUpdateMaxBatches = 2
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	codec := rpcClient(t, s1)
	defer codec.Close()
	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	nodes := []string{"node1", "node2"}
	if err := registerNodes(nodes, codec); err != nil {
		t.Fatal(err)
	}

	arg1 := structs.CoordinateUpdateRequest{
		Datacenter: "dc1",
		Node:       "node1",
		Coord:      generateRandomCoordinate(),
	}
	var out struct{}
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &arg1, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	arg2 := structs.CoordinateUpdateRequest{
		Datacenter: "dc1",
		Node:       "node2",
		Coord:      generateRandomCoordinate(),
	}
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &arg2, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	state := s1.fsm.State()
	_, c, err := state.Coordinate("node1", nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	verify.Values(t, "", c, lib.CoordinateSet{})

	_, c, err = state.Coordinate("node2", nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	verify.Values(t, "", c, lib.CoordinateSet{})

	arg2.Coord = generateRandomCoordinate()
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &arg2, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	time.Sleep(3 * s1.config.CoordinateUpdatePeriod)
	_, c, err = state.Coordinate("node1", nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	expected := lib.CoordinateSet{
		"": arg1.Coord,
	}
	verify.Values(t, "", c, expected)

	_, c, err = state.Coordinate("node2", nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	expected = lib.CoordinateSet{
		"": arg2.Coord,
	}
	verify.Values(t, "", c, expected)

	spamLen := s1.config.CoordinateUpdateBatchSize*s1.config.CoordinateUpdateMaxBatches + 1
	for i := 0; i < spamLen; i++ {
		req := structs.RegisterRequest{
			Datacenter: "dc1",
			Node:       fmt.Sprintf("bogusnode%d", i),
			Address:    "127.0.0.1",
		}
		var reply struct{}
		if err := msgpackrpc.CallWithCodec(codec, "Catalog.Register", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	for i := 0; i < spamLen; i++ {
		arg1.Node = fmt.Sprintf("bogusnode%d", i)
		arg1.Coord = generateRandomCoordinate()
		if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &arg1, &out); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	time.Sleep(3 * s1.config.CoordinateUpdatePeriod)
	numDropped := 0
	for i := 0; i < spamLen; i++ {
		_, c, err = state.Coordinate(fmt.Sprintf("bogusnode%d", i), nil)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(c) == 0 {
			numDropped++
		}
	}
	if numDropped != 1 {
		t.Fatalf("wrong number of coordinates dropped, %d != 1", numDropped)
	}

	arg2.Coord.Vec[0] = math.NaN()
	err = msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &arg2, &out)
	if err == nil || !strings.Contains(err.Error(), "invalid coordinate") {
		t.Fatalf("should have failed with an error, got %v", err)
	}

	arg2.Coord.Vec = make([]float64, 2*len(arg2.Coord.Vec))
	err = msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &arg2, &out)
	if err == nil || !strings.Contains(err.Error(), "incompatible coordinate") {
		t.Fatalf("should have failed with an error, got %v", err)
	}
}

func TestCoordinate_Update_ACLDeny(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
		c.ACLEnforceVersion8 = false
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	nodes := []string{"node1", "node2"}
	if err := registerNodes(nodes, codec); err != nil {
		t.Fatal(err)
	}

	req := structs.CoordinateUpdateRequest{
		Datacenter: "dc1",
		Node:       "node1",
		Coord:      generateRandomCoordinate(),
	}
	var out struct{}
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &req, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	s1.config.ACLEnforceVersion8 = true
	err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &req, &out)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("err: %v", err)
	}

	arg := structs.ACLRequest{
		Datacenter: "dc1",
		Op:         structs.ACLSet,
		ACL: structs.ACL{
			Name: "User token",
			Type: structs.ACLTypeClient,
			Rules: `
node "node1" {
	policy = "write"
}
`,
		},
		WriteRequest: structs.WriteRequest{Token: "root"},
	}
	var id string
	if err := msgpackrpc.CallWithCodec(codec, "ACL.Apply", &arg, &id); err != nil {
		t.Fatalf("err: %v", err)
	}

	req.Token = id
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &req, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	req.Node = "node2"
	err = msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &req, &out)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("err: %v", err)
	}
}

func TestCoordinate_ListDatacenters(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	var out []structs.DatacenterMap
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.ListDatacenters", struct{}{}, &out); err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(out) != 1 ||
		out[0].Datacenter != "dc1" ||
		len(out[0].Coordinates) != 1 ||
		out[0].Coordinates[0].Node != s1.config.NodeName {
		t.Fatalf("bad: %v", out)
	}
	c, err := s1.serfWAN.GetCoordinate()
	if err != nil {
		t.Fatalf("bad: %v", err)
	}
	verify.Values(t, "", c, out[0].Coordinates[0].Coord)
}

func TestCoordinate_ListNodes(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	codec := rpcClient(t, s1)
	defer codec.Close()
	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	nodes := []string{"foo", "bar", "baz"}
	if err := registerNodes(nodes, codec); err != nil {
		t.Fatal(err)
	}

	arg1 := structs.CoordinateUpdateRequest{
		Datacenter: "dc1",
		Node:       "foo",
		Coord:      generateRandomCoordinate(),
	}
	var out struct{}
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &arg1, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	arg2 := structs.CoordinateUpdateRequest{
		Datacenter: "dc1",
		Node:       "bar",
		Coord:      generateRandomCoordinate(),
	}
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &arg2, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	arg3 := structs.CoordinateUpdateRequest{
		Datacenter: "dc1",
		Node:       "baz",
		Coord:      generateRandomCoordinate(),
	}
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &arg3, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	retry.Run(t, func(r *retry.R) {
		arg := structs.DCSpecificRequest{
			Datacenter: "dc1",
		}
		resp := structs.IndexedCoordinates{}
		if err := msgpackrpc.CallWithCodec(codec, "Coordinate.ListNodes", &arg, &resp); err != nil {
			r.Fatalf("err: %v", err)
		}
		if len(resp.Coordinates) != 3 ||
			resp.Coordinates[0].Node != "bar" ||
			resp.Coordinates[1].Node != "baz" ||
			resp.Coordinates[2].Node != "foo" {
			r.Fatalf("bad: %v", resp.Coordinates)
		}
		verify.Values(t, "", resp.Coordinates[0].Coord, arg2.Coord) // bar
		verify.Values(t, "", resp.Coordinates[1].Coord, arg3.Coord) // baz
		verify.Values(t, "", resp.Coordinates[2].Coord, arg1.Coord) // foo
	})
}

func TestCoordinate_ListNodes_ACLFilter(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
		c.ACLEnforceVersion8 = false
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	nodes := []string{"foo", "bar", "baz"}
	for _, node := range nodes {
		req := structs.RegisterRequest{
			Datacenter: "dc1",
			Node:       node,
			Address:    "127.0.0.1",
			WriteRequest: structs.WriteRequest{
				Token: "root",
			},
		}
		var reply struct{}
		if err := msgpackrpc.CallWithCodec(codec, "Catalog.Register", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	arg1 := structs.CoordinateUpdateRequest{
		Datacenter: "dc1",
		Node:       "foo",
		Coord:      generateRandomCoordinate(),
		WriteRequest: structs.WriteRequest{
			Token: "root",
		},
	}
	var out struct{}
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &arg1, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	arg2 := structs.CoordinateUpdateRequest{
		Datacenter: "dc1",
		Node:       "bar",
		Coord:      generateRandomCoordinate(),
		WriteRequest: structs.WriteRequest{
			Token: "root",
		},
	}
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &arg2, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	arg3 := structs.CoordinateUpdateRequest{
		Datacenter: "dc1",
		Node:       "baz",
		Coord:      generateRandomCoordinate(),
		WriteRequest: structs.WriteRequest{
			Token: "root",
		},
	}
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &arg3, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	retry.Run(t, func(r *retry.R) {
		arg := structs.DCSpecificRequest{
			Datacenter: "dc1",
		}
		resp := structs.IndexedCoordinates{}
		if err := msgpackrpc.CallWithCodec(codec, "Coordinate.ListNodes", &arg, &resp); err != nil {
			r.Fatalf("err: %v", err)
		}
		if got, want := len(resp.Coordinates), 3; got != want {
			r.Fatalf("got %d coordinates want %d", got, want)
		}
	})

	s1.config.ACLEnforceVersion8 = true
	arg := structs.DCSpecificRequest{
		Datacenter: "dc1",
	}
	resp := structs.IndexedCoordinates{}
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.ListNodes", &arg, &resp); err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(resp.Coordinates) != 0 {
		t.Fatalf("bad: %#v", resp.Coordinates)
	}

	var id string
	{
		req := structs.ACLRequest{
			Datacenter: "dc1",
			Op:         structs.ACLSet,
			ACL: structs.ACL{
				Name: "User token",
				Type: structs.ACLTypeClient,
				Rules: `
node "foo" {
	policy = "read"
}
`,
			},
			WriteRequest: structs.WriteRequest{Token: "root"},
		}
		if err := msgpackrpc.CallWithCodec(codec, "ACL.Apply", &req, &id); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	arg.Token = id
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.ListNodes", &arg, &resp); err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(resp.Coordinates) != 1 || resp.Coordinates[0].Node != "foo" {
		t.Fatalf("bad: %#v", resp.Coordinates)
	}
}

func TestCoordinate_Node(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	codec := rpcClient(t, s1)
	defer codec.Close()
	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	nodes := []string{"foo", "bar"}
	if err := registerNodes(nodes, codec); err != nil {
		t.Fatal(err)
	}

	arg1 := structs.CoordinateUpdateRequest{
		Datacenter: "dc1",
		Node:       "foo",
		Coord:      generateRandomCoordinate(),
	}
	var out struct{}
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &arg1, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	arg2 := structs.CoordinateUpdateRequest{
		Datacenter: "dc1",
		Node:       "bar",
		Coord:      generateRandomCoordinate(),
	}
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &arg2, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	retry.Run(t, func(r *retry.R) {
		arg := structs.NodeSpecificRequest{
			Node:       "foo",
			Datacenter: "dc1",
		}
		resp := structs.IndexedCoordinates{}
		if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Node", &arg, &resp); err != nil {
			r.Fatalf("err: %v", err)
		}
		if len(resp.Coordinates) != 1 ||
			resp.Coordinates[0].Node != "foo" {
			r.Fatalf("bad: %v", resp.Coordinates)
		}
		verify.Values(t, "", resp.Coordinates[0].Coord, arg1.Coord) // foo
	})
}

func TestCoordinate_Node_ACLDeny(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
		c.ACLEnforceVersion8 = false
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	nodes := []string{"node1", "node2"}
	if err := registerNodes(nodes, codec); err != nil {
		t.Fatal(err)
	}

	coord := generateRandomCoordinate()
	req := structs.CoordinateUpdateRequest{
		Datacenter: "dc1",
		Node:       "node1",
		Coord:      coord,
	}
	var out struct{}
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Update", &req, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	arg := structs.NodeSpecificRequest{
		Node:       "node1",
		Datacenter: "dc1",
	}
	resp := structs.IndexedCoordinates{}
	retry.Run(t, func(r *retry.R) {
		if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Node", &arg, &resp); err != nil {
			r.Fatalf("err: %v", err)
		}
		if len(resp.Coordinates) != 1 ||
			resp.Coordinates[0].Node != "node1" {
			r.Fatalf("bad: %v", resp.Coordinates)
		}
		verify.Values(t, "", resp.Coordinates[0].Coord, coord)
	})

	s1.config.ACLEnforceVersion8 = true
	err := msgpackrpc.CallWithCodec(codec, "Coordinate.Node", &arg, &resp)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("err: %v", err)
	}

	aclReq := structs.ACLRequest{
		Datacenter: "dc1",
		Op:         structs.ACLSet,
		ACL: structs.ACL{
			Name: "User token",
			Type: structs.ACLTypeClient,
			Rules: `
node "node1" {
	policy = "read"
}
`,
		},
		WriteRequest: structs.WriteRequest{Token: "root"},
	}
	var id string
	if err := msgpackrpc.CallWithCodec(codec, "ACL.Apply", &aclReq, &id); err != nil {
		t.Fatalf("err: %v", err)
	}

	arg.Token = id
	if err := msgpackrpc.CallWithCodec(codec, "Coordinate.Node", &arg, &resp); err != nil {
		t.Fatalf("err: %v", err)
	}

	arg.Node = "node2"
	err = msgpackrpc.CallWithCodec(codec, "Coordinate.Node", &arg, &resp)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("err: %v", err)
	}
}

func registerNodes(nodes []string, codec rpc.ClientCodec) error {
	for _, node := range nodes {
		req := structs.RegisterRequest{
			Datacenter: "dc1",
			Node:       node,
			Address:    "127.0.0.1",
		}
		var reply struct{}
		if err := msgpackrpc.CallWithCodec(codec, "Catalog.Register", &req, &reply); err != nil {
			return err
		}
	}

	return nil
}
