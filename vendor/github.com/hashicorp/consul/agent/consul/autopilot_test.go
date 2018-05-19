package consul

import (
	"os"
	"testing"
	"time"

	"github.com/hashicorp/consul/testrpc"
	"github.com/hashicorp/consul/testutil/retry"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

func TestAutopilot_IdempotentShutdown(t *testing.T) {
	dir1, s1 := testServerWithConfig(t, nil)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	retry.Run(t, func(r *retry.R) { r.Check(waitForLeader(s1)) })

	s1.autopilot.Start()
	s1.autopilot.Start()
	s1.autopilot.Start()
	s1.autopilot.Stop()
	s1.autopilot.Stop()
	s1.autopilot.Stop()
}

func TestAutopilot_CleanupDeadServer(t *testing.T) {
	t.Parallel()
	for i := 1; i <= 3; i++ {
		testCleanupDeadServer(t, i)
	}
}

func testCleanupDeadServer(t *testing.T, raftVersion int) {
	conf := func(c *Config) {
		c.Datacenter = "dc1"
		c.Bootstrap = false
		c.BootstrapExpect = 3
		c.RaftConfig.ProtocolVersion = raft.ProtocolVersion(raftVersion)
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

	dir4, s4 := testServerWithConfig(t, conf)
	defer os.RemoveAll(dir4)
	defer s4.Shutdown()

	s3.Shutdown()
	retry.Run(t, func(r *retry.R) {
		alive := 0
		for _, m := range s1.LANMembers() {
			if m.Status == serf.StatusAlive {
				alive++
			}
		}
		if alive != 2 {
			r.Fatal(nil)
		}
	})

	joinLAN(t, s4, s1)
	servers[2] = s4

	for _, s := range servers {
		retry.Run(t, func(r *retry.R) { r.Check(wantPeers(s, 3)) })
	}
}

func TestAutopilot_CleanupDeadServerPeriodic(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.Datacenter = "dc1"
		c.Bootstrap = true
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	conf := func(c *Config) {
		c.Datacenter = "dc1"
		c.Bootstrap = false
	}

	dir2, s2 := testServerWithConfig(t, conf)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	dir3, s3 := testServerWithConfig(t, conf)
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()

	dir4, s4 := testServerWithConfig(t, conf)
	defer os.RemoveAll(dir4)
	defer s4.Shutdown()

	dir5, s5 := testServerWithConfig(t, conf)
	defer os.RemoveAll(dir5)
	defer s5.Shutdown()

	servers := []*Server{s1, s2, s3, s4, s5}

	for _, s := range servers[1:] {
		joinLAN(t, s, s1)
	}
	retry.Run(t, func(r *retry.R) {
		r.Check(wantRaft(servers))
		for _, s := range servers {
			r.Check(wantPeers(s, 5))
		}
	})

	s4.Shutdown()

	servers = []*Server{s1, s2, s3, s5}
	retry.Run(t, func(r *retry.R) {
		r.Check(wantRaft(servers))
		for _, s := range servers {
			r.Check(wantPeers(s, 4))
		}
	})
}

func TestAutopilot_RollingUpdate(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.Datacenter = "dc1"
		c.Bootstrap = true
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	conf := func(c *Config) {
		c.Datacenter = "dc1"
		c.Bootstrap = false
	}

	dir2, s2 := testServerWithConfig(t, conf)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	dir3, s3 := testServerWithConfig(t, conf)
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()

	servers := []*Server{s1, s2, s3}
	for _, s := range servers[1:] {
		joinLAN(t, s, s1)
	}
	retry.Run(t, func(r *retry.R) {
		r.Check(wantRaft(servers))
		for _, s := range servers {
			r.Check(wantPeers(s, 3))
		}
	})

	dir4, s4 := testServerWithConfig(t, conf)
	defer os.RemoveAll(dir4)
	defer s4.Shutdown()
	joinLAN(t, s1, s4)
	servers = append(servers, s4)
	retry.Run(t, func(r *retry.R) {
		r.Check(wantRaft(servers))
		for _, s := range servers {
			r.Check(wantPeers(s, 3))
		}
	})

	s3.Shutdown()

	isVoter := func() bool {
		future := s1.raft.GetConfiguration()
		if err := future.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
		for _, s := range future.Configuration().Servers {
			if string(s.ID) == string(s4.config.NodeID) {
				return s.Suffrage == raft.Voter
			}
		}
		t.Fatalf("didn't find s4")
		return false
	}

	servers = []*Server{s1, s2, s4}
	retry.Run(t, func(r *retry.R) {
		r.Check(wantRaft(servers))
		for _, s := range servers {
			r.Check(wantPeers(s, 3))
		}
		if !isVoter() {
			r.Fatalf("should be a voter")
		}
	})
}

func TestAutopilot_CleanupStaleRaftServer(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerDCBootstrap(t, "dc1", true)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, s2 := testServerDCBootstrap(t, "dc1", false)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	dir3, s3 := testServerDCBootstrap(t, "dc1", false)
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()

	dir4, s4 := testServerDCBootstrap(t, "dc1", false)
	defer os.RemoveAll(dir4)
	defer s4.Shutdown()

	servers := []*Server{s1, s2, s3}

	for _, s := range servers[1:] {
		joinLAN(t, s, s1)
	}

	for _, s := range servers {
		retry.Run(t, func(r *retry.R) { r.Check(wantPeers(s, 3)) })
	}

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	s1.raft.AddVoter(raft.ServerID(s4.config.NodeID), raft.ServerAddress(joinAddrLAN(s4)), 0, 0)

	peers, err := s1.numPeers()
	if err != nil {
		t.Fatal(err)
	}
	if peers != 4 {
		t.Fatalf("bad: %v", peers)
	}

	for _, s := range []*Server{s1, s2, s3} {
		retry.Run(t, func(r *retry.R) { r.Check(wantPeers(s, 3)) })
	}
}

func TestAutopilot_PromoteNonVoter(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.Datacenter = "dc1"
		c.Bootstrap = true
		c.RaftConfig.ProtocolVersion = 3
		c.AutopilotConfig.ServerStabilizationTime = 200 * time.Millisecond
		c.ServerHealthInterval = 100 * time.Millisecond
		c.AutopilotInterval = 100 * time.Millisecond
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()
	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	dir2, s2 := testServerWithConfig(t, func(c *Config) {
		c.Datacenter = "dc1"
		c.Bootstrap = false
		c.RaftConfig.ProtocolVersion = 3
	})
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()
	joinLAN(t, s2, s1)

	retry.Run(t, func(r *retry.R) {
		future := s1.raft.GetConfiguration()
		if err := future.Error(); err != nil {
			r.Fatal(err)
		}

		servers := future.Configuration().Servers
		if len(servers) != 2 {
			r.Fatalf("bad: %v", servers)
		}
		if servers[1].Suffrage != raft.Nonvoter {
			r.Fatalf("bad: %v", servers)
		}
		health := s1.autopilot.GetServerHealth(string(servers[1].ID))
		if health == nil {
			r.Fatal("nil health")
		}
		if !health.Healthy {
			r.Fatalf("bad: %v", health)
		}
		if time.Since(health.StableSince) < s1.config.AutopilotConfig.ServerStabilizationTime/2 {
			r.Fatal("stable period not elapsed")
		}
	})

	retry.Run(t, func(r *retry.R) {
		future := s1.raft.GetConfiguration()
		if err := future.Error(); err != nil {
			r.Fatal(err)
		}

		servers := future.Configuration().Servers
		if len(servers) != 2 {
			r.Fatalf("bad: %v", servers)
		}
		if servers[1].Suffrage != raft.Voter {
			r.Fatalf("bad: %v", servers)
		}
	})
}
