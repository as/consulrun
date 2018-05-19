package router

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/hashicorp/consul/agent/metadata"
	"github.com/hashicorp/serf/serf"
)

type FloodAddrFn func(*metadata.Server) (string, bool)

type FloodPortFn func(*metadata.Server) (int, bool)

func FloodJoins(logger *log.Logger, addrFn FloodAddrFn, portFn FloodPortFn,
	localDatacenter string, localSerf *serf.Serf, globalSerf *serf.Serf) {

	suffix := fmt.Sprintf(".%s", localDatacenter)

	index := make(map[string]*metadata.Server)
	for _, m := range globalSerf.Members() {
		ok, server := metadata.IsConsulServer(m)
		if !ok {
			continue
		}

		if server.Datacenter != localDatacenter {
			continue
		}

		localName := strings.TrimSuffix(server.Name, suffix)
		index[localName] = server
	}

	for _, m := range localSerf.Members() {
		if m.Status != serf.StatusAlive {
			continue
		}

		ok, server := metadata.IsConsulServer(m)
		if !ok {
			continue
		}

		if _, ok := index[server.Name]; ok {
			continue
		}

		addr, _, err := net.SplitHostPort(server.Addr.String())
		if err != nil {
			logger.Printf("[DEBUG] consul: Failed to flood-join %q (bad address %q): %v",
				server.Name, server.Addr.String(), err)
		}
		if addrFn != nil {
			if a, ok := addrFn(server); ok {
				addr = a
			}
		}

		if port, ok := portFn(server); ok {
			addr = net.JoinHostPort(addr, fmt.Sprintf("%d", port))
		} else {

			if ip := net.ParseIP(addr); ip != nil {
				if ip.To4() == nil {
					addr = fmt.Sprintf("[%s]", addr)
				}
			} else {
				logger.Printf("[DEBUG] consul: Failed to parse IP %s", addr)
			}
		}

		n, err := globalSerf.Join([]string{addr}, true)
		if err != nil {
			logger.Printf("[DEBUG] consul: Failed to flood-join %q at %s: %v",
				server.Name, addr, err)
		} else if n > 0 {
			logger.Printf("[DEBUG] consul: Successfully performed flood-join for %q at %s",
				server.Name, addr)
		}
	}
}
