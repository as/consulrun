package testrpc

import (
	"testing"

	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/testutil/retry"
)

type rpcFn func(string, interface{}, interface{}) error

func WaitForLeader(t *testing.T, rpc rpcFn, dc string) {
	var out structs.IndexedNodes
	retry.Run(t, func(r *retry.R) {

		args := &structs.DCSpecificRequest{Datacenter: dc}
		if err := rpc("Catalog.ListNodes", args, &out); err != nil {
			r.Fatalf("Catalog.ListNodes failed: %v", err)
		}
		if !out.QueryMeta.KnownLeader {
			r.Fatalf("No leader")
		}
		if out.Index == 0 {
			r.Fatalf("Consul index is 0")
		}
	})
}
