package agent

import (
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/types"
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
