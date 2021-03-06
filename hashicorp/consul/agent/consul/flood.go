package consul

import (
	"time"

	"github.com/as/consulrun/hashicorp/consul/agent/router"
	"github.com/as/consulrun/hashicorp/serf/serf"
)

func (s *Server) FloodNotify() {
	s.floodLock.RLock()
	defer s.floodLock.RUnlock()

	for _, ch := range s.floodCh {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (s *Server) Flood(addrFn router.FloodAddrFn, portFn router.FloodPortFn, global *serf.Serf) {
	s.floodLock.Lock()
	floodCh := make(chan struct{})
	s.floodCh = append(s.floodCh, floodCh)
	s.floodLock.Unlock()

	ticker := time.NewTicker(s.config.SerfFloodInterval)
	defer ticker.Stop()
	defer func() {
		s.floodLock.Lock()
		defer s.floodLock.Unlock()

		for i, ch := range s.floodCh {
			if ch == floodCh {
				s.floodCh = append(s.floodCh[:i], s.floodCh[i+1:]...)
				return
			}
		}
		panic("flood channels out of sync")
	}()

	for {
		select {
		case <-s.serfLAN.ShutdownCh():
			return

		case <-global.ShutdownCh():
			return

		case <-ticker.C:
			goto FLOOD

		case <-floodCh:
			goto FLOOD
		}

	FLOOD:
		router.FloodJoins(s.logger, addrFn, portFn, s.config.Datacenter, s.serfLAN, global)
	}
}
