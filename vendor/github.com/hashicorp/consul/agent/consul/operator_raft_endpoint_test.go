package consul

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/as/consulrun/hashicorp/consul/acl"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/lib/freeport"
	"github.com/as/consulrun/hashicorp/consul/testrpc"
	"github.com/as/consulrun/hashicorp/net-rpc-msgpackrpc"
	"github.com/as/consulrun/hashicorp/raft"
	"github.com/pascaldekloe/goe/verify"
)

func TestOperator_RaftGetConfiguration(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	arg := structs.DCSpecificRequest{
		Datacenter: "dc1",
	}
	var reply structs.RaftConfigurationResponse
	if err := msgpackrpc.CallWithCodec(codec, "Operator.RaftGetConfiguration", &arg, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	future := s1.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(future.Configuration().Servers) != 1 {
		t.Fatalf("bad: %v", future.Configuration().Servers)
	}
	me := future.Configuration().Servers[0]
	expected := structs.RaftConfigurationResponse{
		Servers: []*structs.RaftServer{
			{
				ID:              me.ID,
				Node:            s1.config.NodeName,
				Address:         me.Address,
				Leader:          true,
				Voter:           true,
				ProtocolVersion: "3",
			},
		},
		Index: future.Index(),
	}
	verify.Values(t, "", reply, expected)
}

func TestOperator_RaftGetConfiguration_ACLDeny(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	arg := structs.DCSpecificRequest{
		Datacenter: "dc1",
	}
	var reply structs.RaftConfigurationResponse
	err := msgpackrpc.CallWithCodec(codec, "Operator.RaftGetConfiguration", &arg, &reply)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("err: %v", err)
	}

	var token string
	{
		var rules = `
                    operator = "read"
                `

		req := structs.ACLRequest{
			Datacenter: "dc1",
			Op:         structs.ACLSet,
			ACL: structs.ACL{
				Name:  "User token",
				Type:  structs.ACLTypeClient,
				Rules: rules,
			},
			WriteRequest: structs.WriteRequest{Token: "root"},
		}
		if err := msgpackrpc.CallWithCodec(codec, "ACL.Apply", &req, &token); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	arg.Token = token
	if err := msgpackrpc.CallWithCodec(codec, "Operator.RaftGetConfiguration", &arg, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	future := s1.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(future.Configuration().Servers) != 1 {
		t.Fatalf("bad: %v", future.Configuration().Servers)
	}
	me := future.Configuration().Servers[0]
	expected := structs.RaftConfigurationResponse{
		Servers: []*structs.RaftServer{
			{
				ID:              me.ID,
				Node:            s1.config.NodeName,
				Address:         me.Address,
				Leader:          true,
				Voter:           true,
				ProtocolVersion: "3",
			},
		},
		Index: future.Index(),
	}
	verify.Values(t, "", reply, expected)
}

func TestOperator_RaftRemovePeerByAddress(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	arg := structs.RaftRemovePeerRequest{
		Datacenter: "dc1",
		Address:    raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", freeport.Get(1)[0])),
	}
	var reply struct{}
	err := msgpackrpc.CallWithCodec(codec, "Operator.RaftRemovePeerByAddress", &arg, &reply)
	if err == nil || !strings.Contains(err.Error(), "not found in the Raft configuration") {
		t.Fatalf("err: %v", err)
	}

	{
		id := raft.ServerID("fake-node-id")
		future := s1.raft.AddVoter(id, arg.Address, 0, time.Second)
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	{
		future := s1.raft.GetConfiguration()
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
		configuration := future.Configuration()
		if len(configuration.Servers) != 2 {
			t.Fatalf("bad: %v", configuration)
		}
	}

	if err := msgpackrpc.CallWithCodec(codec, "Operator.RaftRemovePeerByAddress", &arg, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		future := s1.raft.GetConfiguration()
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
		configuration := future.Configuration()
		if len(configuration.Servers) != 1 {
			t.Fatalf("bad: %v", configuration)
		}
	}
}

func TestOperator_RaftRemovePeerByAddress_ACLDeny(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	arg := structs.RaftRemovePeerRequest{
		Datacenter: "dc1",
		Address:    raft.ServerAddress(s1.config.RPCAddr.String()),
	}
	var reply struct{}
	err := msgpackrpc.CallWithCodec(codec, "Operator.RaftRemovePeerByAddress", &arg, &reply)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("err: %v", err)
	}

	var token string
	{
		var rules = `
                    operator = "write"
                `

		req := structs.ACLRequest{
			Datacenter: "dc1",
			Op:         structs.ACLSet,
			ACL: structs.ACL{
				Name:  "User token",
				Type:  structs.ACLTypeClient,
				Rules: rules,
			},
			WriteRequest: structs.WriteRequest{Token: "root"},
		}
		if err := msgpackrpc.CallWithCodec(codec, "ACL.Apply", &req, &token); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	arg.Token = token
	err = msgpackrpc.CallWithCodec(codec, "Operator.RaftRemovePeerByAddress", &arg, &reply)
	if err == nil || !strings.Contains(err.Error(), "at least one voter") {
		t.Fatalf("err: %v", err)
	}
}

func TestOperator_RaftRemovePeerByID(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.RaftConfig.ProtocolVersion = 3
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	arg := structs.RaftRemovePeerRequest{
		Datacenter: "dc1",
		ID:         raft.ServerID("e35bde83-4e9c-434f-a6ef-453f44ee21ea"),
	}
	var reply struct{}
	err := msgpackrpc.CallWithCodec(codec, "Operator.RaftRemovePeerByID", &arg, &reply)
	if err == nil || !strings.Contains(err.Error(), "not found in the Raft configuration") {
		t.Fatalf("err: %v", err)
	}

	{
		future := s1.raft.AddVoter(arg.ID, raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", freeport.Get(1)[0])), 0, 0)
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	{
		future := s1.raft.GetConfiguration()
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
		configuration := future.Configuration()
		if len(configuration.Servers) != 2 {
			t.Fatalf("bad: %v", configuration)
		}
	}

	if err := msgpackrpc.CallWithCodec(codec, "Operator.RaftRemovePeerByID", &arg, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		future := s1.raft.GetConfiguration()
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
		configuration := future.Configuration()
		if len(configuration.Servers) != 1 {
			t.Fatalf("bad: %v", configuration)
		}
	}
}

func TestOperator_RaftRemovePeerByID_ACLDeny(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
		c.RaftConfig.ProtocolVersion = 3
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	arg := structs.RaftRemovePeerRequest{
		Datacenter: "dc1",
		ID:         raft.ServerID(s1.config.NodeID),
	}
	var reply struct{}
	err := msgpackrpc.CallWithCodec(codec, "Operator.RaftRemovePeerByID", &arg, &reply)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("err: %v", err)
	}

	var token string
	{
		var rules = `
                    operator = "write"
                `

		req := structs.ACLRequest{
			Datacenter: "dc1",
			Op:         structs.ACLSet,
			ACL: structs.ACL{
				Name:  "User token",
				Type:  structs.ACLTypeClient,
				Rules: rules,
			},
			WriteRequest: structs.WriteRequest{Token: "root"},
		}
		if err := msgpackrpc.CallWithCodec(codec, "ACL.Apply", &req, &token); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	arg.Token = token
	err = msgpackrpc.CallWithCodec(codec, "Operator.RaftRemovePeerByID", &arg, &reply)
	if err == nil || !strings.Contains(err.Error(), "at least one voter") {
		t.Fatalf("err: %v", err)
	}
}
