package consul

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/as/consulrun/hashicorp/consul/agent/metadata"
	"github.com/as/consulrun/hashicorp/consul/lib"
	"github.com/as/consulrun/hashicorp/serf/serf"
)

func (c *Client) setupSerf(conf *serf.Config, ch chan serf.Event, path string) (*serf.Serf, error) {
	conf.Init()

	conf.NodeName = c.config.NodeName
	conf.Tags["role"] = "node"
	conf.Tags["dc"] = c.config.Datacenter
	conf.Tags["segment"] = c.config.Segment
	conf.Tags["id"] = string(c.config.NodeID)
	conf.Tags["vsn"] = fmt.Sprintf("%d", c.config.ProtocolVersion)
	conf.Tags["vsn_min"] = fmt.Sprintf("%d", ProtocolVersionMin)
	conf.Tags["vsn_max"] = fmt.Sprintf("%d", ProtocolVersionMax)
	conf.Tags["build"] = c.config.Build
	if c.logger == nil {
		conf.MemberlistConfig.LogOutput = c.config.LogOutput
		conf.LogOutput = c.config.LogOutput
	}
	conf.MemberlistConfig.Logger = c.logger
	conf.Logger = c.logger
	conf.EventCh = ch
	conf.ProtocolVersion = protocolVersionMap[c.config.ProtocolVersion]
	conf.RejoinAfterLeave = c.config.RejoinAfterLeave
	conf.Merge = &lanMergeDelegate{
		dc:       c.config.Datacenter,
		nodeID:   c.config.NodeID,
		nodeName: c.config.NodeName,
		segment:  c.config.Segment,
	}

	conf.SnapshotPath = filepath.Join(c.config.DataDir, path)
	if err := lib.EnsurePath(conf.SnapshotPath, false); err != nil {
		return nil, err
	}

	return serf.Create(conf)
}

func (c *Client) lanEventHandler() {
	var numQueuedEvents int
	for {
		numQueuedEvents = len(c.eventCh)
		if numQueuedEvents > serfEventBacklogWarning {
			c.logger.Printf("[WARN] consul: number of queued serf events above warning threshold: %d/%d", numQueuedEvents, serfEventBacklogWarning)
		}

		select {
		case e := <-c.eventCh:
			switch e.EventType() {
			case serf.EventMemberJoin:
				c.nodeJoin(e.(serf.MemberEvent))
			case serf.EventMemberLeave, serf.EventMemberFailed:
				c.nodeFail(e.(serf.MemberEvent))
			case serf.EventUser:
				c.localEvent(e.(serf.UserEvent))
			case serf.EventMemberUpdate: // Ignore
			case serf.EventMemberReap: // Ignore
			case serf.EventQuery: // Ignore
			default:
				c.logger.Printf("[WARN] consul: unhandled LAN Serf Event: %#v", e)
			}
		case <-c.shutdownCh:
			return
		}
	}
}

func (c *Client) nodeJoin(me serf.MemberEvent) {
	for _, m := range me.Members {
		ok, parts := metadata.IsConsulServer(m)
		if !ok {
			continue
		}
		if parts.Datacenter != c.config.Datacenter {
			c.logger.Printf("[WARN] consul: server %s for datacenter %s has joined wrong cluster",
				m.Name, parts.Datacenter)
			continue
		}
		c.logger.Printf("[INFO] consul: adding server %s", parts)
		c.routers.AddServer(parts)

		if c.config.ServerUp != nil {
			c.config.ServerUp()
		}
	}
}

func (c *Client) nodeFail(me serf.MemberEvent) {
	for _, m := range me.Members {
		ok, parts := metadata.IsConsulServer(m)
		if !ok {
			continue
		}
		c.logger.Printf("[INFO] consul: removing server %s", parts)
		c.routers.RemoveServer(parts)
	}
}

func (c *Client) localEvent(event serf.UserEvent) {

	if !strings.HasPrefix(event.Name, "consul:") {
		return
	}

	switch name := event.Name; {
	case name == newLeaderEvent:
		c.logger.Printf("[INFO] consul: New leader elected: %s", event.Payload)

		if c.config.ServerUp != nil {
			c.config.ServerUp()
		}
	case isUserEvent(name):
		event.Name = rawUserEventName(name)
		c.logger.Printf("[DEBUG] consul: user event: %s", event.Name)

		if c.config.UserEventHandler != nil {
			c.config.UserEventHandler(event)
		}
	default:
		c.logger.Printf("[WARN] consul: Unhandled local event: %v", event)
	}
}
