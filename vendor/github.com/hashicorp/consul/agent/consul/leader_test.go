package consul

import (
	"os"
	"testing"
	"time"

	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/api"
	"github.com/as/consulrun/hashicorp/consul/testrpc"
	"github.com/as/consulrun/hashicorp/consul/testutil/retry"
	"github.com/as/consulrun/hashicorp/net-rpc-msgpackrpc"
	"github.com/as/consulrun/hashicorp/serf/serf"
)

func TestLeader_RegisterMember(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
		c.ACLEnforceVersion8 = true
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, c1 := testClient(t)
	defer os.RemoveAll(dir2)
	defer c1.Shutdown()

	joinLAN(t, c1, s1)

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	state := s1.fsm.State()
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(c1.config.NodeName)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("client not registered")
		}
	})

	_, checks, err := state.NodeChecks(nil, c1.config.NodeName)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(checks) != 1 {
		t.Fatalf("client missing check")
	}
	if checks[0].CheckID != structs.SerfCheckID {
		t.Fatalf("bad check: %v", checks[0])
	}
	if checks[0].Name != structs.SerfCheckName {
		t.Fatalf("bad check: %v", checks[0])
	}
	if checks[0].Status != api.HealthPassing {
		t.Fatalf("bad check: %v", checks[0])
	}

	_, node, err := state.GetNode(s1.config.NodeName)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if node == nil {
		t.Fatalf("server not registered")
	}

	_, services, err := state.NodeServices(nil, s1.config.NodeName)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if _, ok := services.Services["consul"]; !ok {
		t.Fatalf("consul service not registered: %v", services)
	}
}

func TestLeader_FailedMember(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
		c.ACLEnforceVersion8 = true
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, c1 := testClient(t)
	defer os.RemoveAll(dir2)
	defer c1.Shutdown()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	joinLAN(t, c1, s1)

	c1.Shutdown()

	state := s1.fsm.State()
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(c1.config.NodeName)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("client not registered")
		}
	})

	_, checks, err := state.NodeChecks(nil, c1.config.NodeName)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(checks) != 1 {
		t.Fatalf("client missing check")
	}
	if checks[0].CheckID != structs.SerfCheckID {
		t.Fatalf("bad check: %v", checks[0])
	}
	if checks[0].Name != structs.SerfCheckName {
		t.Fatalf("bad check: %v", checks[0])
	}

	retry.Run(t, func(r *retry.R) {
		_, checks, err = state.NodeChecks(nil, c1.config.NodeName)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if got, want := checks[0].Status, api.HealthCritical; got != want {
			r.Fatalf("got status %q want %q", got, want)
		}
	})
}

func TestLeader_LeftMember(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
		c.ACLEnforceVersion8 = true
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, c1 := testClient(t)
	defer os.RemoveAll(dir2)
	defer c1.Shutdown()

	joinLAN(t, c1, s1)

	state := s1.fsm.State()

	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(c1.config.NodeName)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("client not registered")
		}
	})

	c1.Leave()
	c1.Shutdown()

	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(c1.config.NodeName)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node != nil {
			r.Fatal("client still registered")
		}
	})
}
func TestLeader_ReapMember(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
		c.ACLEnforceVersion8 = true
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, c1 := testClient(t)
	defer os.RemoveAll(dir2)
	defer c1.Shutdown()

	joinLAN(t, c1, s1)

	state := s1.fsm.State()

	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(c1.config.NodeName)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("client not registered")
		}
	})

	mems := s1.LANMembers()
	var c1mem serf.Member
	for _, m := range mems {
		if m.Name == c1.config.NodeName {
			c1mem = m
			c1mem.Status = StatusReap
			break
		}
	}
	s1.reconcileCh <- c1mem

	reaped := false
	for start := time.Now(); time.Since(start) < 5*time.Second; {
		_, node, err := state.GetNode(c1.config.NodeName)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if node == nil {
			reaped = true
			break
		}
	}
	if !reaped {
		t.Fatalf("client should not be registered")
	}
}

func TestLeader_ReapServer(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "allow"
		c.ACLEnforceVersion8 = true
		c.Bootstrap = true
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, s2 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "allow"
		c.ACLEnforceVersion8 = true
		c.Bootstrap = false
	})
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	dir3, s3 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "allow"
		c.ACLEnforceVersion8 = true
		c.Bootstrap = false
	})
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()

	joinLAN(t, s1, s2)
	joinLAN(t, s1, s3)

	testrpc.WaitForLeader(t, s1.RPC, "dc1")
	state := s1.fsm.State()

	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(s3.config.NodeName)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("client not registered")
		}
	})

	knownMembers := make(map[string]struct{})
	knownMembers[s1.config.NodeName] = struct{}{}
	knownMembers[s2.config.NodeName] = struct{}{}

	err := s1.reconcileReaped(knownMembers)

	if err != nil {
		t.Fatalf("Unexpected error :%v", err)
	}

	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(s3.config.NodeName)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node != nil {
			r.Fatalf("server with id %v should not be registered", s3.config.NodeID)
		}
	})

}

func TestLeader_Reconcile_ReapMember(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
		c.ACLEnforceVersion8 = true
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	dead := structs.RegisterRequest{
		Datacenter: s1.config.Datacenter,
		Node:       "no-longer-around",
		Address:    "127.1.1.1",
		Check: &structs.HealthCheck{
			Node:    "no-longer-around",
			CheckID: structs.SerfCheckID,
			Name:    structs.SerfCheckName,
			Status:  api.HealthCritical,
		},
		WriteRequest: structs.WriteRequest{
			Token: "root",
		},
	}
	var out struct{}
	if err := s1.RPC("Catalog.Register", &dead, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := s1.reconcile(); err != nil {
		t.Fatalf("err: %v", err)
	}

	state := s1.fsm.State()
	_, node, err := state.GetNode("no-longer-around")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if node != nil {
		t.Fatalf("client registered")
	}
}

func TestLeader_Reconcile(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
		c.ACLEnforceVersion8 = true
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, c1 := testClient(t)
	defer os.RemoveAll(dir2)
	defer c1.Shutdown()

	joinLAN(t, c1, s1)

	state := s1.fsm.State()
	_, node, err := state.GetNode(c1.config.NodeName)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if node != nil {
		t.Fatalf("client registered")
	}

	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(c1.config.NodeName)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("client not registered")
		}
	})
}

func TestLeader_Reconcile_Races(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	dir2, c1 := testClient(t)
	defer os.RemoveAll(dir2)
	defer c1.Shutdown()

	joinLAN(t, c1, s1)

	state := s1.fsm.State()
	var nodeAddr string
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(c1.config.NodeName)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node == nil {
			r.Fatal("client not registered")
		}
		nodeAddr = node.Address
	})

	req := structs.RegisterRequest{
		Datacenter: s1.config.Datacenter,
		Node:       c1.config.NodeName,
		ID:         c1.config.NodeID,
		Address:    nodeAddr,
		NodeMeta:   map[string]string{"hello": "world"},
		Check: &structs.HealthCheck{
			Node:    c1.config.NodeName,
			CheckID: structs.SerfCheckID,
			Name:    structs.SerfCheckName,
			Status:  api.HealthCritical,
			Output:  "",
		},
	}
	var out struct{}
	if err := s1.RPC("Catalog.Register", &req, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := s1.reconcile(); err != nil {
		t.Fatalf("err: %v", err)
	}
	_, node, err := state.GetNode(c1.config.NodeName)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if node == nil {
		t.Fatalf("bad")
	}
	if hello, ok := node.Meta["hello"]; !ok || hello != "world" {
		t.Fatalf("bad")
	}

	c1.Shutdown()
	retry.Run(t, func(r *retry.R) {
		_, checks, err := state.NodeChecks(nil, c1.config.NodeName)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if got, want := checks[0].Status, api.HealthCritical; got != want {
			r.Fatalf("got state %q want %q", got, want)
		}
	})

	_, node, err = state.GetNode(c1.config.NodeName)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if node == nil {
		t.Fatalf("bad")
	}
	if hello, ok := node.Meta["hello"]; !ok || hello != "world" {
		t.Fatalf("bad")
	}
}

func TestLeader_LeftServer(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, s2 := testServerDCBootstrap(t, "dc1", false)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	dir3, s3 := testServerDCBootstrap(t, "dc1", false)
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()

	servers := []*Server{s2, s3, s1}

	joinLAN(t, s2, s1)
	joinLAN(t, s3, s1)
	for _, s := range servers {
		retry.Run(t, func(r *retry.R) { r.Check(wantPeers(s, 3)) })
	}

	servers[0].Shutdown()

	if err := servers[1].RemoveFailedNode(servers[0].config.NodeName); err != nil {
		t.Fatalf("err: %v", err)
	}

	for _, s := range servers[1:] {
		retry.Run(t, func(r *retry.R) { r.Check(wantPeers(s, 2)) })
	}
	s1.Shutdown()
}

func TestLeader_LeftLeader(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, s2 := testServerDCBootstrap(t, "dc1", false)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	dir3, s3 := testServerDCBootstrap(t, "dc1", false)
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()
	servers := []*Server{s1, s2, s3}

	joinLAN(t, s2, s1)
	joinLAN(t, s3, s1)

	for _, s := range servers {
		retry.Run(t, func(r *retry.R) { r.Check(wantPeers(s, 3)) })
	}

	var leader *Server
	for _, s := range servers {
		if s.IsLeader() {
			leader = s
			break
		}
	}
	if leader == nil {
		t.Fatalf("Should have a leader")
	}
	if !leader.isReadyForConsistentReads() {
		t.Fatalf("Expected leader to be ready for consistent reads ")
	}
	leader.Leave()
	if leader.isReadyForConsistentReads() {
		t.Fatalf("Expected consistent read state to be false ")
	}
	leader.Shutdown()
	time.Sleep(100 * time.Millisecond)

	var remain *Server
	for _, s := range servers {
		if s == leader {
			continue
		}
		remain = s
		retry.Run(t, func(r *retry.R) { r.Check(wantPeers(s, 2)) })
	}

	state := remain.fsm.State()
	retry.Run(t, func(r *retry.R) {
		_, node, err := state.GetNode(leader.config.NodeName)
		if err != nil {
			r.Fatalf("err: %v", err)
		}
		if node != nil {
			r.Fatal("leader should be deregistered")
		}
	})
}

func TestLeader_MultiBootstrap(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, s2 := testServer(t)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	servers := []*Server{s1, s2}

	joinLAN(t, s2, s1)

	for _, s := range servers {
		retry.Run(t, func(r *retry.R) {
			if got, want := len(s.serfLAN.Members()), 2; got != want {
				r.Fatalf("got %d peers want %d", got, want)
			}
		})
	}

	for _, s := range servers {
		peers, _ := s.numPeers()
		if peers != 1 {
			t.Fatalf("should only have 1 raft peer!")
		}
	}
}

func TestLeader_TombstoneGC_Reset(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, s2 := testServerDCBootstrap(t, "dc1", false)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	dir3, s3 := testServerDCBootstrap(t, "dc1", false)
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()
	servers := []*Server{s1, s2, s3}

	joinLAN(t, s2, s1)
	joinLAN(t, s3, s1)

	for _, s := range servers {
		retry.Run(t, func(r *retry.R) { r.Check(wantPeers(s, 3)) })
	}

	var leader *Server
	for _, s := range servers {
		if s.IsLeader() {
			leader = s
			break
		}
	}
	if leader == nil {
		t.Fatalf("Should have a leader")
	}

	if !leader.tombstoneGC.PendingExpiration() {
		t.Fatalf("should have pending expiration")
	}

	leader.Shutdown()
	time.Sleep(100 * time.Millisecond)

	leader = nil
	retry.Run(t, func(r *retry.R) {
		for _, s := range servers {
			if s.IsLeader() {
				leader = s
				return
			}
		}
		r.Fatal("no leader")
	})

	retry.Run(t, func(r *retry.R) {
		if !leader.tombstoneGC.PendingExpiration() {
			r.Fatal("leader has no pending GC expiration")
		}
	})
}

func TestLeader_ReapTombstones(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
		c.TombstoneTTL = 50 * time.Millisecond
		c.TombstoneTTLGranularity = 10 * time.Millisecond
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	arg := structs.KVSRequest{
		Datacenter: "dc1",
		Op:         api.KVSet,
		DirEnt: structs.DirEntry{
			Key:   "test",
			Value: []byte("test"),
		},
		WriteRequest: structs.WriteRequest{
			Token: "root",
		},
	}
	var out bool
	if err := msgpackrpc.CallWithCodec(codec, "KVS.Apply", &arg, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	arg.Op = api.KVDelete
	if err := msgpackrpc.CallWithCodec(codec, "KVS.Apply", &arg, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	state := s1.fsm.State()
	func() {
		snap := state.Snapshot()
		defer snap.Close()
		stones, err := snap.Tombstones()
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if stones.Next() == nil {
			t.Fatalf("missing tombstones")
		}
		if stones.Next() != nil {
			t.Fatalf("unexpected extra tombstones")
		}
	}()

	retry.Run(t, func(r *retry.R) {
		snap := state.Snapshot()
		defer snap.Close()
		stones, err := snap.Tombstones()
		if err != nil {
			r.Fatal(err)
		}
		if stones.Next() != nil {
			r.Fatal("should have no tombstones")
		}
	})
}

func TestLeader_RollRaftServer(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.Bootstrap = true
		c.Datacenter = "dc1"
		c.RaftConfig.ProtocolVersion = 2
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, s2 := testServerWithConfig(t, func(c *Config) {
		c.Bootstrap = false
		c.Datacenter = "dc1"
		c.RaftConfig.ProtocolVersion = 1
	})
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	dir3, s3 := testServerWithConfig(t, func(c *Config) {
		c.Bootstrap = false
		c.Datacenter = "dc1"
		c.RaftConfig.ProtocolVersion = 2
	})
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()

	servers := []*Server{s1, s2, s3}

	joinLAN(t, s2, s1)
	joinLAN(t, s3, s1)

	for _, s := range servers {
		retry.Run(t, func(r *retry.R) { r.Check(wantPeers(s, 3)) })
	}

	s2.Shutdown()

	for _, s := range []*Server{s1, s3} {
		retry.Run(t, func(r *retry.R) {
			minVer, err := s.autopilot.MinRaftProtocol()
			if err != nil {
				r.Fatal(err)
			}
			if got, want := minVer, 2; got != want {
				r.Fatalf("got min raft version %d want %d", got, want)
			}
		})
	}

	dir4, s4 := testServerWithConfig(t, func(c *Config) {
		c.Bootstrap = false
		c.Datacenter = "dc1"
		c.RaftConfig.ProtocolVersion = 3
	})
	defer os.RemoveAll(dir4)
	defer s4.Shutdown()
	joinLAN(t, s4, s1)
	servers[1] = s4

	for _, s := range servers {
		retry.Run(t, func(r *retry.R) {
			addrs := 0
			ids := 0
			future := s.raft.GetConfiguration()
			if err := future.Error(); err != nil {
				r.Fatal(err)
			}
			for _, server := range future.Configuration().Servers {
				if string(server.ID) == string(server.Address) {
					addrs++
				} else {
					ids++
				}
			}
			if got, want := addrs, 2; got != want {
				r.Fatalf("got %d server addresses want %d", got, want)
			}
			if got, want := ids, 1; got != want {
				r.Fatalf("got %d server ids want %d", got, want)
			}
		})
	}
}

func TestLeader_ChangeServerID(t *testing.T) {
	t.Parallel()
	conf := func(c *Config) {
		c.Bootstrap = false
		c.BootstrapExpect = 3
		c.Datacenter = "dc1"
		c.RaftConfig.ProtocolVersion = 3
	}
	dir1, s1 := testServerWithConfig(t, conf)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, s2 := testServerWithConfig(t, conf)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	dir3, s3 := testServerWithConfig(t, conf)
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()

	servers := []*Server{s1, s2, s3}

	joinLAN(t, s2, s1)
	joinLAN(t, s3, s1)
	for _, s := range servers {
		retry.Run(t, func(r *retry.R) { r.Check(wantPeers(s, 3)) })
	}

	s3.Shutdown()

	retry.Run(t, func(r *retry.R) {
		alive := 0
		for _, m := range s1.LANMembers() {
			if m.Status == serf.StatusAlive {
				alive++
			}
		}
		if got, want := alive, 2; got != want {
			r.Fatalf("got %d alive members want %d", got, want)
		}
	})

	dir4, s4 := testServerWithConfig(t, func(c *Config) {
		c.Bootstrap = false
		c.BootstrapExpect = 3
		c.Datacenter = "dc1"
		c.RaftConfig.ProtocolVersion = 3
		c.SerfLANConfig.MemberlistConfig = s3.config.SerfLANConfig.MemberlistConfig
		c.RPCAddr = s3.config.RPCAddr
		c.RPCAdvertise = s3.config.RPCAdvertise
	})
	defer os.RemoveAll(dir4)
	defer s4.Shutdown()
	joinLAN(t, s4, s1)
	servers[2] = s4

	retry.Run(t, func(r *retry.R) {
		r.Check(wantRaft(servers))
		for _, s := range servers {
			r.Check(wantPeers(s, 3))
		}
	})
}

func TestLeader_ACL_Initialization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		build     string
		master    string
		init      bool
		bootstrap bool
	}{
		{"old version, no master", "0.8.0", "", false, false},
		{"old version, master", "0.8.0", "root", false, false},
		{"new version, no master", "0.9.1", "", true, true},
		{"new version, master", "0.9.1", "root", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := func(c *Config) {
				c.Build = tt.build
				c.Bootstrap = true
				c.Datacenter = "dc1"
				c.ACLDatacenter = "dc1"
				c.ACLMasterToken = tt.master
			}
			dir1, s1 := testServerWithConfig(t, conf)
			defer os.RemoveAll(dir1)
			defer s1.Shutdown()
			testrpc.WaitForLeader(t, s1.RPC, "dc1")

			if tt.master != "" {
				_, master, err := s1.fsm.State().ACLGet(nil, tt.master)
				if err != nil {
					t.Fatalf("err: %v", err)
				}
				if master == nil {
					t.Fatalf("master token wasn't created")
				}
			}

			_, anon, err := s1.fsm.State().ACLGet(nil, anonymousToken)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if anon == nil {
				t.Fatalf("anonymous token wasn't created")
			}

			bs, err := s1.fsm.State().ACLGetBootstrap()
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if !tt.init {
				if bs != nil {
					t.Fatalf("bootstrap should not be initialized")
				}
			} else {
				if bs == nil {
					t.Fatalf("bootstrap should be initialized")
				}
				if got, want := bs.AllowBootstrap, tt.bootstrap; got != want {
					t.Fatalf("got %v want %v", got, want)
				}
			}
		})
	}
}
