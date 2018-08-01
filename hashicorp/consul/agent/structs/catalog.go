package structs

import (
	"github.com/as/consulrun/hashicorp/consul/types"
)

const (
	SerfCheckID           types.CheckID = "serfHealth"
	SerfCheckName                       = "Serf Health Status"
	SerfCheckAliveOutput                = "Agent alive and reachable"
	SerfCheckFailedOutput               = "Agent not live or unreachable"
)

const (
	ConsulServiceID   = "consul"
	ConsulServiceName = "consul"
)
