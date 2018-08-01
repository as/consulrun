package consul

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/as/consulrun/hashicorp/consul/agent/metadata"
	"github.com/as/consulrun/hashicorp/consul/testrpc"
	"github.com/as/consulrun/hashicorp/consul/types"
)

func TestStatsFetcher(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerDCExpect(t, "dc1", 3)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, s2 := testServerDCExpect(t, "dc1", 3)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	dir3, s3 := testServerDCExpect(t, "dc1", 3)
	defer os.RemoveAll(dir3)
	defer s3.Shutdown()

	joinLAN(t, s2, s1)
	joinLAN(t, s3, s1)
	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	members := s1.serfLAN.Members()
	if len(members) != 3 {
		t.Fatalf("bad len: %d", len(members))
	}

	var servers []*metadata.Server
	for _, member := range members {
		ok, server := metadata.IsConsulServer(member)
		if !ok {
			t.Fatalf("bad: %#v", member)
		}
		servers = append(servers, server)
	}

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		stats := s1.statsFetcher.Fetch(ctx, s1.LANMembers())
		if len(stats) != 3 {
			t.Fatalf("bad: %#v", stats)
		}
		for id, stat := range stats {
			switch types.NodeID(id) {
			case s1.config.NodeID, s2.config.NodeID, s3.config.NodeID:

			default:
				t.Fatalf("bad: %s", id)
			}

			if stat == nil || stat.LastTerm == 0 {
				t.Fatalf("bad: %#v", stat)
			}
		}
	}()

	func() {
		s1.statsFetcher.inflight[string(s3.config.NodeID)] = struct{}{}
		defer delete(s1.statsFetcher.inflight, string(s3.config.NodeID))

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		stats := s1.statsFetcher.Fetch(ctx, s1.LANMembers())
		if len(stats) != 2 {
			t.Fatalf("bad: %#v", stats)
		}
		for id, stat := range stats {
			switch types.NodeID(id) {
			case s1.config.NodeID, s2.config.NodeID:

			case s3.config.NodeID:
				t.Fatalf("bad")
			default:
				t.Fatalf("bad: %s", id)
			}

			if stat == nil || stat.LastTerm == 0 {
				t.Fatalf("bad: %#v", stat)
			}
		}
	}()
}
