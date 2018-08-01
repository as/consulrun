package consul

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/as/consulrun/hashicorp/consul/acl"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/testrpc"
	"github.com/as/consulrun/hashicorp/consul/testutil/retry"
)

var testACLPolicy = `
key "" {
	policy = "deny"
}
key "foo/" {
	policy = "write"
}
`

func TestACL_Disabled(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	client := rpcClient(t, s1)
	defer client.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	acl, err := s1.resolveToken("does not exist")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if acl != nil {
		t.Fatalf("got acl")
	}
}

func TestACL_ResolveRootACL(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1" // Enable ACLs!
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	rule, err := s1.resolveToken("allow")
	if !acl.IsErrRootDenied(err) {
		t.Fatalf("err: %v", err)
	}
	if rule != nil {
		t.Fatalf("bad: %v", rule)
	}

	rule, err = s1.resolveToken("deny")
	if !acl.IsErrRootDenied(err) {
		t.Fatalf("err: %v", err)
	}
	if rule != nil {
		t.Fatalf("bad: %v", rule)
	}
}

func TestACL_Authority_NotFound(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1" // Enable ACLs!
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	client := rpcClient(t, s1)
	defer client.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	rule, err := s1.resolveToken("does not exist")
	if !acl.IsErrNotFound(err) {
		t.Fatalf("err: %v", err)
	}
	if rule != nil {
		t.Fatalf("got acl")
	}
}

func TestACL_Authority_Found(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1" // Enable ACLs!
		c.ACLMasterToken = "root"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	client := rpcClient(t, s1)
	defer client.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	arg := structs.ACLRequest{
		Datacenter: "dc1",
		Op:         structs.ACLSet,
		ACL: structs.ACL{
			Name:  "User token",
			Type:  structs.ACLTypeClient,
			Rules: testACLPolicy,
		},
		WriteRequest: structs.WriteRequest{Token: "root"},
	}
	var id string
	if err := s1.RPC("ACL.Apply", &arg, &id); err != nil {
		t.Fatalf("err: %v", err)
	}

	acl, err := s1.resolveToken(id)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if acl == nil {
		t.Fatalf("missing acl")
	}

	if acl.KeyRead("bar") {
		t.Fatalf("unexpected read")
	}
	if !acl.KeyRead("foo/test") {
		t.Fatalf("unexpected failed read")
	}
}

func TestACL_Authority_Anonymous_Found(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1" // Enable ACLs!
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	client := rpcClient(t, s1)
	defer client.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	acl, err := s1.resolveToken("")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if acl == nil {
		t.Fatalf("missing acl")
	}

	if !acl.KeyRead("foo/test") {
		t.Fatalf("unexpected failed read")
	}
}

func TestACL_Authority_Master_Found(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1" // Enable ACLs!
		c.ACLMasterToken = "foobar"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	client := rpcClient(t, s1)
	defer client.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	acl, err := s1.resolveToken("foobar")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if acl == nil {
		t.Fatalf("missing acl")
	}

	if !acl.KeyRead("foo/test") {
		t.Fatalf("unexpected failed read")
	}
}

func TestACL_Authority_Management(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1" // Enable ACLs!
		c.ACLMasterToken = "foobar"
		c.ACLDefaultPolicy = "deny"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	client := rpcClient(t, s1)
	defer client.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	acl, err := s1.resolveToken("foobar")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if acl == nil {
		t.Fatalf("missing acl")
	}

	if !acl.KeyRead("foo/test") {
		t.Fatalf("unexpected failed read")
	}
}

func TestACL_NonAuthority_NotFound(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, s2 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1" // Enable ACLs!
		c.Bootstrap = false     // Disable bootstrap
	})
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	joinLAN(t, s2, s1)
	retry.Run(t, func(r *retry.R) { r.Check(wantRaft([]*Server{s1, s2})) })

	client := rpcClient(t, s1)
	defer client.Close()
	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	var nonAuth *Server
	if !s1.IsLeader() {
		nonAuth = s1
	} else {
		nonAuth = s2
	}

	rule, err := nonAuth.resolveToken("does not exist")
	if !acl.IsErrNotFound(err) {
		t.Fatalf("err: %v", err)
	}
	if rule != nil {
		t.Fatalf("got acl")
	}
}

func TestACL_NonAuthority_Found(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	client := rpcClient(t, s1)
	defer client.Close()

	dir2, s2 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1" // Enable ACLs!
		c.Bootstrap = false     // Disable bootstrap
	})
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	joinLAN(t, s2, s1)
	retry.Run(t, func(r *retry.R) { r.Check(wantRaft([]*Server{s1, s2})) })

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	arg := structs.ACLRequest{
		Datacenter: "dc1",
		Op:         structs.ACLSet,
		ACL: structs.ACL{
			Name:  "User token",
			Type:  structs.ACLTypeClient,
			Rules: testACLPolicy,
		},
		WriteRequest: structs.WriteRequest{Token: "root"},
	}
	var id string
	if err := s1.RPC("ACL.Apply", &arg, &id); err != nil {
		t.Fatalf("err: %v", err)
	}

	var nonAuth *Server
	if !s1.IsLeader() {
		nonAuth = s1
	} else {
		nonAuth = s2
	}

	acl, err := nonAuth.resolveToken(id)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if acl == nil {
		t.Fatalf("missing acl")
	}

	if acl.KeyRead("bar") {
		t.Fatalf("unexpected read")
	}
	if !acl.KeyRead("foo/test") {
		t.Fatalf("unexpected failed read")
	}
}

func TestACL_NonAuthority_Management(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1" // Enable ACLs!
		c.ACLMasterToken = "foobar"
		c.ACLDefaultPolicy = "deny"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	client := rpcClient(t, s1)
	defer client.Close()

	dir2, s2 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1" // Enable ACLs!
		c.ACLDefaultPolicy = "deny"
		c.Bootstrap = false // Disable bootstrap
	})
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	joinLAN(t, s2, s1)
	retry.Run(t, func(r *retry.R) { r.Check(wantRaft([]*Server{s1, s2})) })

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	var nonAuth *Server
	if !s1.IsLeader() {
		nonAuth = s1
	} else {
		nonAuth = s2
	}

	acl, err := nonAuth.resolveToken("foobar")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if acl == nil {
		t.Fatalf("missing acl")
	}

	if !acl.KeyRead("foo/test") {
		t.Fatalf("unexpected failed read")
	}
}

func TestACL_DownPolicy_Deny(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLDownPolicy = "deny"
		c.ACLMasterToken = "root"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	client := rpcClient(t, s1)
	defer client.Close()

	dir2, s2 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1" // Enable ACLs!
		c.ACLDownPolicy = "deny"
		c.Bootstrap = false // Disable bootstrap
	})
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	joinLAN(t, s2, s1)
	retry.Run(t, func(r *retry.R) { r.Check(wantRaft([]*Server{s1, s2})) })

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	arg := structs.ACLRequest{
		Datacenter: "dc1",
		Op:         structs.ACLSet,
		ACL: structs.ACL{
			Name:  "User token",
			Type:  structs.ACLTypeClient,
			Rules: testACLPolicy,
		},
		WriteRequest: structs.WriteRequest{Token: "root"},
	}
	var id string
	if err := s1.RPC("ACL.Apply", &arg, &id); err != nil {
		t.Fatalf("err: %v", err)
	}

	var nonAuth *Server
	var auth *Server
	if !s1.IsLeader() {
		nonAuth = s1
		auth = s2
	} else {
		nonAuth = s2
		auth = s1
	}

	auth.Shutdown()

	aclR, err := nonAuth.resolveToken(id)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if aclR != acl.DenyAll() {
		t.Fatalf("bad acl: %#v", aclR)
	}
}

func TestACL_DownPolicy_Allow(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLDownPolicy = "allow"
		c.ACLMasterToken = "root"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	client := rpcClient(t, s1)
	defer client.Close()

	dir2, s2 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1" // Enable ACLs!
		c.ACLDownPolicy = "allow"
		c.Bootstrap = false // Disable bootstrap
	})
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	joinLAN(t, s2, s1)
	retry.Run(t, func(r *retry.R) { r.Check(wantRaft([]*Server{s1, s2})) })

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	arg := structs.ACLRequest{
		Datacenter: "dc1",
		Op:         structs.ACLSet,
		ACL: structs.ACL{
			Name:  "User token",
			Type:  structs.ACLTypeClient,
			Rules: testACLPolicy,
		},
		WriteRequest: structs.WriteRequest{Token: "root"},
	}
	var id string
	if err := s1.RPC("ACL.Apply", &arg, &id); err != nil {
		t.Fatalf("err: %v", err)
	}

	var nonAuth *Server
	var auth *Server
	if !s1.IsLeader() {
		nonAuth = s1
		auth = s2
	} else {
		nonAuth = s2
		auth = s1
	}

	auth.Shutdown()

	aclR, err := nonAuth.resolveToken(id)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if aclR != acl.AllowAll() {
		t.Fatalf("bad acl: %#v", aclR)
	}
}

func TestACL_DownPolicy_ExtendCache(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLTTL = 0
		c.ACLDownPolicy = "extend-cache"
		c.ACLMasterToken = "root"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	client := rpcClient(t, s1)
	defer client.Close()

	dir2, s2 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1" // Enable ACLs!
		c.ACLTTL = 0
		c.ACLDownPolicy = "extend-cache"
		c.Bootstrap = false // Disable bootstrap
	})
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	joinLAN(t, s2, s1)
	retry.Run(t, func(r *retry.R) { r.Check(wantRaft([]*Server{s1, s2})) })

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	arg := structs.ACLRequest{
		Datacenter: "dc1",
		Op:         structs.ACLSet,
		ACL: structs.ACL{
			Name:  "User token",
			Type:  structs.ACLTypeClient,
			Rules: testACLPolicy,
		},
		WriteRequest: structs.WriteRequest{Token: "root"},
	}
	var id string
	if err := s1.RPC("ACL.Apply", &arg, &id); err != nil {
		t.Fatalf("err: %v", err)
	}

	var nonAuth *Server
	var auth *Server
	if !s1.IsLeader() {
		nonAuth = s1
		auth = s2
	} else {
		nonAuth = s2
		auth = s1
	}

	aclR, err := nonAuth.resolveToken(id)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if aclR == nil {
		t.Fatalf("bad acl: %#v", aclR)
	}

	auth.Shutdown()

	aclR2, err := nonAuth.resolveToken(id)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if aclR2 != aclR {
		t.Fatalf("bad acl: %#v", aclR)
	}
}

func TestACL_Replication(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	client := rpcClient(t, s1)
	defer client.Close()

	dir2, s2 := testServerWithConfig(t, func(c *Config) {
		c.Datacenter = "dc2"
		c.ACLDatacenter = "dc1"
		c.ACLDefaultPolicy = "deny"
		c.ACLDownPolicy = "extend-cache"
		c.EnableACLReplication = true
		c.ACLReplicationInterval = 10 * time.Millisecond
		c.ACLReplicationApplyLimit = 1000000
	})
	s2.tokens.UpdateACLReplicationToken("root")
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	dir3, s3 := testServerWithConfig(t, func(c *Config) {
		c.Datacenter = "dc3"
		c.ACLDatacenter = "dc1"
		c.ACLDownPolicy = "deny"
		c.EnableACLReplication = true
		c.ACLReplicationInterval = 10 * time.Millisecond
		c.ACLReplicationApplyLimit = 1000000
	})
	s3.tokens.UpdateACLReplicationToken("root")
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()

	joinWAN(t, s2, s1)
	joinWAN(t, s3, s1)
	testrpc.WaitForLeader(t, s1.RPC, "dc1")
	testrpc.WaitForLeader(t, s1.RPC, "dc2")
	testrpc.WaitForLeader(t, s1.RPC, "dc3")

	arg := structs.ACLRequest{
		Datacenter: "dc1",
		Op:         structs.ACLSet,
		ACL: structs.ACL{
			Name:  "User token",
			Type:  structs.ACLTypeClient,
			Rules: testACLPolicy,
		},
		WriteRequest: structs.WriteRequest{Token: "root"},
	}
	var id string
	if err := s1.RPC("ACL.Apply", &arg, &id); err != nil {
		t.Fatalf("err: %v", err)
	}

	retry.Run(t, func(r *retry.R) {
		_, acl, err := s2.fsm.State().ACLGet(nil, id)
		if err != nil {
			r.Fatal(err)
		}
		if acl == nil {
			r.Fatal(nil)
		}
		_, acl, err = s3.fsm.State().ACLGet(nil, id)
		if err != nil {
			r.Fatal(err)
		}
		if acl == nil {
			r.Fatal(nil)
		}
	})

	s1.Shutdown()

	acl, err := s2.resolveToken(id)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if acl == nil {
		t.Fatalf("missing acl")
	}

	if acl.KeyRead("bar") {
		t.Fatalf("unexpected read")
	}
	if !acl.KeyRead("foo/test") {
		t.Fatalf("unexpected failed read")
	}

	acl, err = s3.resolveToken(id)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if acl == nil {
		t.Fatalf("missing acl")
	}

	if acl.KeyRead("bar") {
		t.Fatalf("unexpected read")
	}
	if acl.KeyRead("foo/test") {
		t.Fatalf("unexpected read")
	}
}

func TestACL_MultiDC_Found(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	client := rpcClient(t, s1)
	defer client.Close()

	dir2, s2 := testServerWithConfig(t, func(c *Config) {
		c.Datacenter = "dc2"
		c.ACLDatacenter = "dc1" // Enable ACLs!
	})
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	joinWAN(t, s2, s1)

	testrpc.WaitForLeader(t, s1.RPC, "dc1")
	testrpc.WaitForLeader(t, s1.RPC, "dc2")

	arg := structs.ACLRequest{
		Datacenter: "dc1",
		Op:         structs.ACLSet,
		ACL: structs.ACL{
			Name:  "User token",
			Type:  structs.ACLTypeClient,
			Rules: testACLPolicy,
		},
		WriteRequest: structs.WriteRequest{Token: "root"},
	}
	var id string
	if err := s1.RPC("ACL.Apply", &arg, &id); err != nil {
		t.Fatalf("err: %v", err)
	}

	acl, err := s2.resolveToken(id)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if acl == nil {
		t.Fatalf("missing acl")
	}

	if acl.KeyRead("bar") {
		t.Fatalf("unexpected read")
	}
	if !acl.KeyRead("foo/test") {
		t.Fatalf("unexpected failed read")
	}
}

func TestACL_filterHealthChecks(t *testing.T) {
	t.Parallel()

	fill := func() structs.HealthChecks {
		return structs.HealthChecks{
			&structs.HealthCheck{
				Node:        "node1",
				CheckID:     "check1",
				ServiceName: "foo",
			},
		}
	}

	{
		hc := fill()
		filt := newACLFilter(acl.AllowAll(), nil, false)
		filt.filterHealthChecks(&hc)
		if len(hc) != 1 {
			t.Fatalf("bad: %#v", hc)
		}
	}

	{
		hc := fill()
		filt := newACLFilter(acl.DenyAll(), nil, false)
		filt.filterHealthChecks(&hc)
		if len(hc) != 0 {
			t.Fatalf("bad: %#v", hc)
		}
	}

	policy, err := acl.Parse(`
service "foo" {
  policy = "read"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err := acl.New(acl.DenyAll(), policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		hc := fill()
		filt := newACLFilter(perms, nil, false)
		filt.filterHealthChecks(&hc)
		if len(hc) != 1 {
			t.Fatalf("bad: %#v", hc)
		}
	}

	{
		hc := fill()
		filt := newACLFilter(perms, nil, true)
		filt.filterHealthChecks(&hc)
		if len(hc) != 0 {
			t.Fatalf("bad: %#v", hc)
		}
	}

	policy, err = acl.Parse(`
node "node1" {
  policy = "read"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err = acl.New(perms, policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		hc := fill()
		filt := newACLFilter(perms, nil, true)
		filt.filterHealthChecks(&hc)
		if len(hc) != 1 {
			t.Fatalf("bad: %#v", hc)
		}
	}
}

func TestACL_filterServices(t *testing.T) {
	t.Parallel()

	services := structs.Services{
		"service1": []string{},
		"service2": []string{},
		"consul":   []string{},
	}

	filt := newACLFilter(acl.AllowAll(), nil, false)
	filt.filterServices(services)
	if len(services) != 3 {
		t.Fatalf("bad: %#v", services)
	}

	filt = newACLFilter(acl.DenyAll(), nil, false)
	filt.filterServices(services)
	if len(services) != 1 {
		t.Fatalf("bad: %#v", services)
	}
	if _, ok := services["consul"]; !ok {
		t.Fatalf("bad: %#v", services)
	}

	filt = newACLFilter(acl.DenyAll(), nil, true)
	filt.filterServices(services)
	if len(services) != 0 {
		t.Fatalf("bad: %#v", services)
	}
}

func TestACL_filterServiceNodes(t *testing.T) {
	t.Parallel()

	fill := func() structs.ServiceNodes {
		return structs.ServiceNodes{
			&structs.ServiceNode{
				Node:        "node1",
				ServiceName: "foo",
			},
		}
	}

	{
		nodes := fill()
		filt := newACLFilter(acl.AllowAll(), nil, false)
		filt.filterServiceNodes(&nodes)
		if len(nodes) != 1 {
			t.Fatalf("bad: %#v", nodes)
		}
	}

	{
		nodes := fill()
		filt := newACLFilter(acl.DenyAll(), nil, false)
		filt.filterServiceNodes(&nodes)
		if len(nodes) != 0 {
			t.Fatalf("bad: %#v", nodes)
		}
	}

	policy, err := acl.Parse(`
service "foo" {
  policy = "read"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err := acl.New(acl.DenyAll(), policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		nodes := fill()
		filt := newACLFilter(perms, nil, false)
		filt.filterServiceNodes(&nodes)
		if len(nodes) != 1 {
			t.Fatalf("bad: %#v", nodes)
		}
	}

	{
		nodes := fill()
		filt := newACLFilter(perms, nil, true)
		filt.filterServiceNodes(&nodes)
		if len(nodes) != 0 {
			t.Fatalf("bad: %#v", nodes)
		}
	}

	policy, err = acl.Parse(`
node "node1" {
  policy = "read"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err = acl.New(perms, policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		nodes := fill()
		filt := newACLFilter(perms, nil, true)
		filt.filterServiceNodes(&nodes)
		if len(nodes) != 1 {
			t.Fatalf("bad: %#v", nodes)
		}
	}
}

func TestACL_filterNodeServices(t *testing.T) {
	t.Parallel()

	fill := func() *structs.NodeServices {
		return &structs.NodeServices{
			Node: &structs.Node{
				Node: "node1",
			},
			Services: map[string]*structs.NodeService{
				"foo": {
					ID:      "foo",
					Service: "foo",
				},
			},
		}
	}

	{
		var services *structs.NodeServices
		filt := newACLFilter(acl.AllowAll(), nil, false)
		filt.filterNodeServices(&services)
		if services != nil {
			t.Fatalf("bad: %#v", services)
		}
	}

	{
		services := fill()
		filt := newACLFilter(acl.AllowAll(), nil, false)
		filt.filterNodeServices(&services)
		if len(services.Services) != 1 {
			t.Fatalf("bad: %#v", services.Services)
		}
	}

	{
		services := fill()
		filt := newACLFilter(acl.DenyAll(), nil, false)
		filt.filterNodeServices(&services)
		if len((*services).Services) != 0 {
			t.Fatalf("bad: %#v", (*services).Services)
		}
	}

	policy, err := acl.Parse(`
service "foo" {
  policy = "read"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err := acl.New(acl.DenyAll(), policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		services := fill()
		filt := newACLFilter(perms, nil, false)
		filt.filterNodeServices(&services)
		if len((*services).Services) != 1 {
			t.Fatalf("bad: %#v", (*services).Services)
		}
	}

	{
		services := fill()
		filt := newACLFilter(perms, nil, true)
		filt.filterNodeServices(&services)
		if services != nil {
			t.Fatalf("bad: %#v", services)
		}
	}

	policy, err = acl.Parse(`
node "node1" {
  policy = "read"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err = acl.New(perms, policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		services := fill()
		filt := newACLFilter(perms, nil, true)
		filt.filterNodeServices(&services)
		if len((*services).Services) != 1 {
			t.Fatalf("bad: %#v", (*services).Services)
		}
	}
}

func TestACL_filterCheckServiceNodes(t *testing.T) {
	t.Parallel()

	fill := func() structs.CheckServiceNodes {
		return structs.CheckServiceNodes{
			structs.CheckServiceNode{
				Node: &structs.Node{
					Node: "node1",
				},
				Service: &structs.NodeService{
					ID:      "foo",
					Service: "foo",
				},
				Checks: structs.HealthChecks{
					&structs.HealthCheck{
						Node:        "node1",
						CheckID:     "check1",
						ServiceName: "foo",
					},
				},
			},
		}
	}

	{
		nodes := fill()
		filt := newACLFilter(acl.AllowAll(), nil, false)
		filt.filterCheckServiceNodes(&nodes)
		if len(nodes) != 1 {
			t.Fatalf("bad: %#v", nodes)
		}
		if len(nodes[0].Checks) != 1 {
			t.Fatalf("bad: %#v", nodes[0].Checks)
		}
	}

	{
		nodes := fill()
		filt := newACLFilter(acl.DenyAll(), nil, false)
		filt.filterCheckServiceNodes(&nodes)
		if len(nodes) != 0 {
			t.Fatalf("bad: %#v", nodes)
		}
	}

	policy, err := acl.Parse(`
service "foo" {
  policy = "read"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err := acl.New(acl.DenyAll(), policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		nodes := fill()
		filt := newACLFilter(perms, nil, false)
		filt.filterCheckServiceNodes(&nodes)
		if len(nodes) != 1 {
			t.Fatalf("bad: %#v", nodes)
		}
		if len(nodes[0].Checks) != 1 {
			t.Fatalf("bad: %#v", nodes[0].Checks)
		}
	}

	{
		nodes := fill()
		filt := newACLFilter(perms, nil, true)
		filt.filterCheckServiceNodes(&nodes)
		if len(nodes) != 0 {
			t.Fatalf("bad: %#v", nodes)
		}
	}

	policy, err = acl.Parse(`
node "node1" {
  policy = "read"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err = acl.New(perms, policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		nodes := fill()
		filt := newACLFilter(perms, nil, true)
		filt.filterCheckServiceNodes(&nodes)
		if len(nodes) != 1 {
			t.Fatalf("bad: %#v", nodes)
		}
		if len(nodes[0].Checks) != 1 {
			t.Fatalf("bad: %#v", nodes[0].Checks)
		}
	}
}

func TestACL_filterCoordinates(t *testing.T) {
	t.Parallel()

	coords := structs.Coordinates{
		&structs.Coordinate{
			Node:  "node1",
			Coord: generateRandomCoordinate(),
		},
		&structs.Coordinate{
			Node:  "node2",
			Coord: generateRandomCoordinate(),
		},
	}

	filt := newACLFilter(acl.AllowAll(), nil, false)
	filt.filterCoordinates(&coords)
	if len(coords) != 2 {
		t.Fatalf("bad: %#v", coords)
	}

	filt = newACLFilter(acl.DenyAll(), nil, false)
	filt.filterCoordinates(&coords)
	if len(coords) != 2 {
		t.Fatalf("bad: %#v", coords)
	}

	filt = newACLFilter(acl.DenyAll(), nil, true)
	filt.filterCoordinates(&coords)
	if len(coords) != 0 {
		t.Fatalf("bad: %#v", coords)
	}
}

func TestACL_filterSessions(t *testing.T) {
	t.Parallel()

	sessions := structs.Sessions{
		&structs.Session{
			Node: "foo",
		},
		&structs.Session{
			Node: "bar",
		},
	}

	filt := newACLFilter(acl.AllowAll(), nil, true)
	filt.filterSessions(&sessions)
	if len(sessions) != 2 {
		t.Fatalf("bad: %#v", sessions)
	}

	filt = newACLFilter(acl.DenyAll(), nil, false)
	filt.filterSessions(&sessions)
	if len(sessions) != 2 {
		t.Fatalf("bad: %#v", sessions)
	}

	filt = newACLFilter(acl.DenyAll(), nil, true)
	filt.filterSessions(&sessions)
	if len(sessions) != 0 {
		t.Fatalf("bad: %#v", sessions)
	}
}

func TestACL_filterNodeDump(t *testing.T) {
	t.Parallel()

	fill := func() structs.NodeDump {
		return structs.NodeDump{
			&structs.NodeInfo{
				Node: "node1",
				Services: []*structs.NodeService{
					{
						ID:      "foo",
						Service: "foo",
					},
				},
				Checks: []*structs.HealthCheck{
					{
						Node:        "node1",
						CheckID:     "check1",
						ServiceName: "foo",
					},
				},
			},
		}
	}

	{
		dump := fill()
		filt := newACLFilter(acl.AllowAll(), nil, false)
		filt.filterNodeDump(&dump)
		if len(dump) != 1 {
			t.Fatalf("bad: %#v", dump)
		}
		if len(dump[0].Services) != 1 {
			t.Fatalf("bad: %#v", dump[0].Services)
		}
		if len(dump[0].Checks) != 1 {
			t.Fatalf("bad: %#v", dump[0].Checks)
		}
	}

	{
		dump := fill()
		filt := newACLFilter(acl.DenyAll(), nil, false)
		filt.filterNodeDump(&dump)
		if len(dump) != 1 {
			t.Fatalf("bad: %#v", dump)
		}
		if len(dump[0].Services) != 0 {
			t.Fatalf("bad: %#v", dump[0].Services)
		}
		if len(dump[0].Checks) != 0 {
			t.Fatalf("bad: %#v", dump[0].Checks)
		}
	}

	policy, err := acl.Parse(`
service "foo" {
  policy = "read"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err := acl.New(acl.DenyAll(), policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		dump := fill()
		filt := newACLFilter(perms, nil, false)
		filt.filterNodeDump(&dump)
		if len(dump) != 1 {
			t.Fatalf("bad: %#v", dump)
		}
		if len(dump[0].Services) != 1 {
			t.Fatalf("bad: %#v", dump[0].Services)
		}
		if len(dump[0].Checks) != 1 {
			t.Fatalf("bad: %#v", dump[0].Checks)
		}
	}

	{
		dump := fill()
		filt := newACLFilter(perms, nil, true)
		filt.filterNodeDump(&dump)
		if len(dump) != 0 {
			t.Fatalf("bad: %#v", dump)
		}
	}

	policy, err = acl.Parse(`
node "node1" {
  policy = "read"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err = acl.New(perms, policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		dump := fill()
		filt := newACLFilter(perms, nil, true)
		filt.filterNodeDump(&dump)
		if len(dump) != 1 {
			t.Fatalf("bad: %#v", dump)
		}
		if len(dump[0].Services) != 1 {
			t.Fatalf("bad: %#v", dump[0].Services)
		}
		if len(dump[0].Checks) != 1 {
			t.Fatalf("bad: %#v", dump[0].Checks)
		}
	}
}

func TestACL_filterNodes(t *testing.T) {
	t.Parallel()

	nodes := structs.Nodes{
		&structs.Node{
			Node: "foo",
		},
		&structs.Node{
			Node: "bar",
		},
	}

	filt := newACLFilter(acl.AllowAll(), nil, true)
	filt.filterNodes(&nodes)
	if len(nodes) != 2 {
		t.Fatalf("bad: %#v", nodes)
	}

	filt = newACLFilter(acl.DenyAll(), nil, false)
	filt.filterNodes(&nodes)
	if len(nodes) != 2 {
		t.Fatalf("bad: %#v", nodes)
	}

	filt = newACLFilter(acl.DenyAll(), nil, true)
	filt.filterNodes(&nodes)
	if len(nodes) != 0 {
		t.Fatalf("bad: %#v", nodes)
	}
}

func TestACL_redactPreparedQueryTokens(t *testing.T) {
	t.Parallel()
	query := &structs.PreparedQuery{
		ID:    "f004177f-2c28-83b7-4229-eacc25fe55d1",
		Token: "root",
	}

	expected := &structs.PreparedQuery{
		ID:    "f004177f-2c28-83b7-4229-eacc25fe55d1",
		Token: "root",
	}

	filt := newACLFilter(acl.ManageAll(), nil, false)
	filt.redactPreparedQueryTokens(&query)
	if !reflect.DeepEqual(query, expected) {
		t.Fatalf("bad: %#v", &query)
	}

	original := query

	filt = newACLFilter(acl.AllowAll(), nil, false)
	filt.redactPreparedQueryTokens(&query)
	expected.Token = redactedToken
	if !reflect.DeepEqual(query, expected) {
		t.Fatalf("bad: %#v", *query)
	}

	if original.Token != "root" {
		t.Fatalf("bad token: %s", original.Token)
	}
}

func TestACL_filterPreparedQueries(t *testing.T) {
	t.Parallel()
	queries := structs.PreparedQueries{
		&structs.PreparedQuery{
			ID: "f004177f-2c28-83b7-4229-eacc25fe55d1",
		},
		&structs.PreparedQuery{
			ID:   "f004177f-2c28-83b7-4229-eacc25fe55d2",
			Name: "query-with-no-token",
		},
		&structs.PreparedQuery{
			ID:    "f004177f-2c28-83b7-4229-eacc25fe55d3",
			Name:  "query-with-a-token",
			Token: "root",
		},
	}

	expected := structs.PreparedQueries{
		&structs.PreparedQuery{
			ID: "f004177f-2c28-83b7-4229-eacc25fe55d1",
		},
		&structs.PreparedQuery{
			ID:   "f004177f-2c28-83b7-4229-eacc25fe55d2",
			Name: "query-with-no-token",
		},
		&structs.PreparedQuery{
			ID:    "f004177f-2c28-83b7-4229-eacc25fe55d3",
			Name:  "query-with-a-token",
			Token: "root",
		},
	}

	filt := newACLFilter(acl.ManageAll(), nil, false)
	filt.filterPreparedQueries(&queries)
	if !reflect.DeepEqual(queries, expected) {
		t.Fatalf("bad: %#v", queries)
	}

	original := queries[2]

	filt = newACLFilter(acl.AllowAll(), nil, false)
	filt.filterPreparedQueries(&queries)
	expected[2].Token = redactedToken
	expected = append(structs.PreparedQueries{}, expected[1], expected[2])
	if !reflect.DeepEqual(queries, expected) {
		t.Fatalf("bad: %#v", queries)
	}

	if original.Token != "root" {
		t.Fatalf("bad token: %s", original.Token)
	}

	filt = newACLFilter(acl.DenyAll(), nil, false)
	filt.filterPreparedQueries(&queries)
	if len(queries) != 0 {
		t.Fatalf("bad: %#v", queries)
	}
}

func TestACL_unhandledFilterType(t *testing.T) {
	t.Parallel()
	defer func(t *testing.T) {
		if recover() == nil {
			t.Fatalf("should panic")
		}
	}(t)

	dir, token, srv, client := testACLFilterServer(t)
	defer os.RemoveAll(dir)
	defer srv.Shutdown()
	defer client.Close()

	srv.filterACL(token, &structs.HealthCheck{})
}

func TestACL_vetRegisterWithACL(t *testing.T) {
	t.Parallel()
	args := &structs.RegisterRequest{
		Node:    "nope",
		Address: "127.0.0.1",
	}

	if err := vetRegisterWithACL(nil, args, nil); err != nil {
		t.Fatalf("err: %v", err)
	}

	policy, err := acl.Parse(`
node "node" {
  policy = "write"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err := acl.New(acl.DenyAll(), policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = vetRegisterWithACL(perms, args, nil)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}

	args.Node = "node"
	if err := vetRegisterWithACL(perms, args, nil); err != nil {
		t.Fatalf("err: %v", err)
	}

	ns := &structs.NodeServices{
		Node: &structs.Node{
			Node:    "node",
			Address: "127.0.0.1",
		},
		Services: make(map[string]*structs.NodeService),
	}

	args.Service = &structs.NodeService{
		Service: "service",
		ID:      "my-id",
	}
	err = vetRegisterWithACL(perms, args, ns)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}

	policy, err = acl.Parse(`
service "service" {
  policy = "write"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err = acl.New(perms, policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := vetRegisterWithACL(perms, args, ns); err != nil {
		t.Fatalf("err: %v", err)
	}

	ns.Services["my-id"] = &structs.NodeService{
		Service: "other",
		ID:      "my-id",
	}
	err = vetRegisterWithACL(perms, args, ns)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}

	policy, err = acl.Parse(`
service "other" {
  policy = "write"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err = acl.New(perms, policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := vetRegisterWithACL(perms, args, ns); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := vetRegisterWithACL(perms, args, nil); err != nil {
		t.Fatalf("err: %v", err)
	}

	args.Check = &structs.HealthCheck{
		Node: "node",
	}
	err = vetRegisterWithACL(perms, args, ns)
	if err == nil || !strings.Contains(err.Error(), "check member must be nil") {
		t.Fatalf("bad: %v", err)
	}

	args.Check.Node = "nope"
	args.Checks = append(args.Checks, args.Check)
	args.Check = nil
	err = vetRegisterWithACL(perms, args, ns)
	if err == nil || !strings.Contains(err.Error(), "doesn't match register request node") {
		t.Fatalf("bad: %v", err)
	}

	args.Checks[0].Node = "node"
	if err := vetRegisterWithACL(perms, args, ns); err != nil {
		t.Fatalf("err: %v", err)
	}

	args.Checks = append(args.Checks, &structs.HealthCheck{
		Node:      "node",
		ServiceID: "my-id",
	})
	if err := vetRegisterWithACL(perms, args, ns); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := vetRegisterWithACL(perms, args, nil); err != nil {
		t.Fatalf("err: %v", err)
	}

	args.Service = nil
	if err := vetRegisterWithACL(perms, args, ns); err != nil {
		t.Fatalf("err: %v", err)
	}

	policy, err = acl.Parse(`
service "other" {
  policy = "deny"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err = acl.New(perms, policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = vetRegisterWithACL(perms, args, ns)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}

	ns.Services["my-id"] = &structs.NodeService{
		Service: "service",
		ID:      "my-id",
	}
	if err := vetRegisterWithACL(perms, args, ns); err != nil {
		t.Fatalf("err: %v", err)
	}

	policy, err = acl.Parse(`
node "node" {
  policy = "deny"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err = acl.New(perms, policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = vetRegisterWithACL(perms, args, ns)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}

	args.Checks[0].ServiceID = "my-id"
	if err := vetRegisterWithACL(perms, args, ns); err != nil {
		t.Fatalf("err: %v", err)
	}

	args.Address = "127.0.0.2"
	err = vetRegisterWithACL(perms, args, ns)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}
}

func TestACL_vetDeregisterWithACL(t *testing.T) {
	t.Parallel()
	args := &structs.DeregisterRequest{
		Node: "nope",
	}

	if err := vetDeregisterWithACL(nil, args, nil, nil); err != nil {
		t.Fatalf("err: %v", err)
	}

	policy, err := acl.Parse(`
node "node" {
  policy = "write"
}
service "service" {
  policy = "write"
}
`, nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}
	perms, err := acl.New(acl.DenyAll(), policy, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	err = vetDeregisterWithACL(perms, args, nil, nil)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}

	args.Node = "node"
	if err := vetDeregisterWithACL(perms, args, nil, nil); err != nil {
		t.Fatalf("err: %v", err)
	}

	args.CheckID = "check-id"
	err = vetDeregisterWithACL(perms, args, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "Unknown check") {
		t.Fatalf("bad: %v", err)
	}

	nc := &structs.HealthCheck{
		Node:        "node",
		CheckID:     "check-id",
		ServiceID:   "service-id",
		ServiceName: "nope",
	}
	err = vetDeregisterWithACL(perms, args, nil, nc)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}

	nc.ServiceName = "service"
	if err := vetDeregisterWithACL(perms, args, nil, nc); err != nil {
		t.Fatalf("err: %v", err)
	}

	args.Node = "nope"
	nc.Node = "nope"
	nc.ServiceID = ""
	nc.ServiceName = ""
	err = vetDeregisterWithACL(perms, args, nil, nc)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}

	args.Node = "node"
	nc.Node = "node"
	if err := vetDeregisterWithACL(perms, args, nil, nc); err != nil {
		t.Fatalf("err: %v", err)
	}

	args.ServiceID = "service-id"
	err = vetDeregisterWithACL(perms, args, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "Unknown service") {
		t.Fatalf("bad: %v", err)
	}

	ns := &structs.NodeService{
		ID:      "service-id",
		Service: "nope",
	}
	err = vetDeregisterWithACL(perms, args, ns, nil)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}

	ns.Service = "service"
	if err := vetDeregisterWithACL(perms, args, ns, nil); err != nil {
		t.Fatalf("err: %v", err)
	}
}
