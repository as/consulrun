package consul

import (
	"context"
	"log"
	"sync"

	"github.com/as/consulrun/hashicorp/consul/agent/consul/autopilot"
	"github.com/as/consulrun/hashicorp/consul/agent/metadata"
	"github.com/as/consulrun/hashicorp/consul/agent/pool"
	"github.com/as/consulrun/hashicorp/serf/serf"
)

type StatsFetcher struct {
	logger       *log.Logger
	pool         *pool.ConnPool
	datacenter   string
	inflight     map[string]struct{}
	inflightLock sync.Mutex
}

func NewStatsFetcher(logger *log.Logger, pool *pool.ConnPool, datacenter string) *StatsFetcher {
	return &StatsFetcher{
		logger:     logger,
		pool:       pool,
		datacenter: datacenter,
		inflight:   make(map[string]struct{}),
	}
}

func (f *StatsFetcher) fetch(server *metadata.Server, replyCh chan *autopilot.ServerStats) {
	var args struct{}
	var reply autopilot.ServerStats
	err := f.pool.RPC(f.datacenter, server.Addr, server.Version, "Status.RaftStats", server.UseTLS, &args, &reply)
	if err != nil {
		f.logger.Printf("[WARN] consul: error getting server health from %q: %v",
			server.Name, err)
	} else {
		replyCh <- &reply
	}

	f.inflightLock.Lock()
	delete(f.inflight, server.ID)
	f.inflightLock.Unlock()
}

func (f *StatsFetcher) Fetch(ctx context.Context, members []serf.Member) map[string]*autopilot.ServerStats {
	type workItem struct {
		server  *metadata.Server
		replyCh chan *autopilot.ServerStats
	}
	var servers []*metadata.Server
	for _, s := range members {
		if ok, parts := metadata.IsConsulServer(s); ok {
			servers = append(servers, parts)
		}
	}

	var work []*workItem
	f.inflightLock.Lock()
	for _, server := range servers {
		if _, ok := f.inflight[server.ID]; ok {
			f.logger.Printf("[WARN] consul: error getting server health from %q: last request still outstanding",
				server.Name)
		} else {
			workItem := &workItem{
				server:  server,
				replyCh: make(chan *autopilot.ServerStats, 1),
			}
			work = append(work, workItem)
			f.inflight[server.ID] = struct{}{}
			go f.fetch(workItem.server, workItem.replyCh)
		}
	}
	f.inflightLock.Unlock()

	replies := make(map[string]*autopilot.ServerStats)
	for _, workItem := range work {
		select {
		case reply := <-workItem.replyCh:
			replies[workItem.server.ID] = reply

		case <-ctx.Done():
			f.logger.Printf("[WARN] consul: error getting server health from %q: %v",
				workItem.server.Name, ctx.Err())
		}
	}
	return replies
}
