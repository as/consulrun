package structs

import (
	"net"

	"github.com/hashicorp/consul/agent/consul/autopilot"
	"github.com/hashicorp/raft"
)

type RaftServer struct {
	ID raft.ServerID

	Node string

	Address raft.ServerAddress

	Leader bool

	ProtocolVersion string

	Voter bool
}

type RaftConfigurationResponse struct {
	Servers []*RaftServer

	Index uint64
}

type RaftRemovePeerRequest struct {
	Datacenter string

	Address raft.ServerAddress

	ID raft.ServerID

	WriteRequest
}

func (op *RaftRemovePeerRequest) RequestDatacenter() string {
	return op.Datacenter
}

type AutopilotSetConfigRequest struct {
	Datacenter string

	Config autopilot.Config

	CAS bool

	WriteRequest
}

func (op *AutopilotSetConfigRequest) RequestDatacenter() string {
	return op.Datacenter
}

type NetworkSegment struct {
	Name string

	Bind *net.TCPAddr

	Advertise *net.TCPAddr

	RPCListener bool
}
