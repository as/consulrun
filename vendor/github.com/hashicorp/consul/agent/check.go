package agent

import (
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/types"
)

type persistedCheck struct {
	Check   *structs.HealthCheck
	ChkType *structs.CheckType
	Token   string
}

type persistedCheckState struct {
	CheckID types.CheckID
	Output  string
	Status  string
	Expires int64
}
