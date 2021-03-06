package structs

import (
	"time"

	"github.com/as/consulrun/hashicorp/consul/api"
	"github.com/as/consulrun/hashicorp/consul/types"
)

type CheckDefinition struct {
	ID        types.CheckID
	Name      string
	Notes     string
	ServiceID string
	Token     string
	Status    string

	//

	//
	Script                         string
	ScriptArgs                     []string
	HTTP                           string
	Header                         map[string][]string
	Method                         string
	TCP                            string
	Interval                       time.Duration
	DockerContainerID              string
	Shell                          string
	GRPC                           string
	GRPCUseTLS                     bool
	TLSSkipVerify                  bool
	Timeout                        time.Duration
	TTL                            time.Duration
	DeregisterCriticalServiceAfter time.Duration
}

func (c *CheckDefinition) HealthCheck(node string) *HealthCheck {
	health := &HealthCheck{
		Node:      node,
		CheckID:   c.ID,
		Name:      c.Name,
		Status:    api.HealthCritical,
		Notes:     c.Notes,
		ServiceID: c.ServiceID,
	}
	if c.Status != "" {
		health.Status = c.Status
	}
	if health.CheckID == "" && health.Name != "" {
		health.CheckID = types.CheckID(health.Name)
	}
	return health
}

func (c *CheckDefinition) CheckType() *CheckType {
	return &CheckType{
		CheckID: c.ID,
		Name:    c.Name,
		Status:  c.Status,
		Notes:   c.Notes,

		Script:            c.Script,
		ScriptArgs:        c.ScriptArgs,
		HTTP:              c.HTTP,
		GRPC:              c.GRPC,
		GRPCUseTLS:        c.GRPCUseTLS,
		Header:            c.Header,
		Method:            c.Method,
		TCP:               c.TCP,
		Interval:          c.Interval,
		DockerContainerID: c.DockerContainerID,
		Shell:             c.Shell,
		TLSSkipVerify:     c.TLSSkipVerify,
		Timeout:           c.Timeout,
		TTL:               c.TTL,
		DeregisterCriticalServiceAfter: c.DeregisterCriticalServiceAfter,
	}
}
