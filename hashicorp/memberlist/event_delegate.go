package memberlist

type EventDelegate interface {
	NotifyJoin(*Node)

	NotifyLeave(*Node)

	NotifyUpdate(*Node)
}

//
type ChannelEventDelegate struct {
	Ch chan<- NodeEvent
}

type NodeEventType int

const (
	NodeJoin NodeEventType = iota
	NodeLeave
	NodeUpdate
)

type NodeEvent struct {
	Event NodeEventType
	Node  *Node
}

func (c *ChannelEventDelegate) NotifyJoin(n *Node) {
	c.Ch <- NodeEvent{NodeJoin, n}
}

func (c *ChannelEventDelegate) NotifyLeave(n *Node) {
	c.Ch <- NodeEvent{NodeLeave, n}
}

func (c *ChannelEventDelegate) NotifyUpdate(n *Node) {
	c.Ch <- NodeEvent{NodeUpdate, n}
}
