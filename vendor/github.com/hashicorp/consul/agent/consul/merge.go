package consul

import (
	"fmt"

	"github.com/hashicorp/consul/agent/metadata"
	"github.com/hashicorp/consul/types"
	"github.com/hashicorp/go-version"
	"github.com/hashicorp/serf/serf"
)

type lanMergeDelegate struct {
	dc       string
	nodeID   types.NodeID
	nodeName string
	segment  string
}

var uniqueIDMinVersion = version.Must(version.NewVersion("0.8.5"))

func (md *lanMergeDelegate) NotifyMerge(members []*serf.Member) error {
	nodeMap := make(map[types.NodeID]string)
	for _, m := range members {
		if rawID, ok := m.Tags["id"]; ok && rawID != "" {
			nodeID := types.NodeID(rawID)

			if (nodeID == md.nodeID) && (m.Name != md.nodeName) {
				return fmt.Errorf("Member '%s' has conflicting node ID '%s' with this agent's ID",
					m.Name, nodeID)
			}

			if other, ok := nodeMap[nodeID]; ok {
				return fmt.Errorf("Member '%s' has conflicting node ID '%s' with member '%s'",
					m.Name, nodeID, other)
			}

			if ver, err := metadata.Build(m); err == nil {
				if ver.Compare(uniqueIDMinVersion) >= 0 {
					nodeMap[nodeID] = m.Name
				}
			}
		}

		if ok, dc := isConsulNode(*m); ok {
			if dc != md.dc {
				return fmt.Errorf("Member '%s' part of wrong datacenter '%s'",
					m.Name, dc)
			}
		}

		if ok, parts := metadata.IsConsulServer(*m); ok {
			if parts.Datacenter != md.dc {
				return fmt.Errorf("Member '%s' part of wrong datacenter '%s'",
					m.Name, parts.Datacenter)
			}
		}

		if segment := m.Tags["segment"]; segment != md.segment {
			return fmt.Errorf("Member '%s' part of wrong segment '%s' (expected '%s')",
				m.Name, segment, md.segment)
		}
	}
	return nil
}

type wanMergeDelegate struct {
}

func (md *wanMergeDelegate) NotifyMerge(members []*serf.Member) error {
	for _, m := range members {
		ok, _ := metadata.IsConsulServer(*m)
		if !ok {
			return fmt.Errorf("Member '%s' is not a server", m.Name)
		}
	}
	return nil
}
