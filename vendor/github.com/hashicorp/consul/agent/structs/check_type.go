package structs

import (
	"fmt"
	"reflect"
	"time"

	"github.com/hashicorp/consul/types"
)

type CheckType struct {
	CheckID types.CheckID
	Name    string
	Status  string
	Notes   string

	Script            string
	ScriptArgs        []string
	HTTP              string
	Header            map[string][]string
	Method            string
	TCP               string
	Interval          time.Duration
	DockerContainerID string
	Shell             string
	GRPC              string
	GRPCUseTLS        bool
	TLSSkipVerify     bool
	Timeout           time.Duration
	TTL               time.Duration

	DeregisterCriticalServiceAfter time.Duration
}
type CheckTypes []*CheckType

func (c *CheckType) Validate() error {
	intervalCheck := c.IsScript() || c.HTTP != "" || c.TCP != "" || c.GRPC != ""

	if c.Interval > 0 && c.TTL > 0 {
		return fmt.Errorf("Interval and TTL cannot both be specified")
	}
	if intervalCheck && c.Interval <= 0 {
		return fmt.Errorf("Interval must be > 0 for Script, HTTP, or TCP checks")
	}
	if !intervalCheck && c.TTL <= 0 {
		return fmt.Errorf("TTL must be > 0 for TTL checks")
	}
	return nil
}

func (c *CheckType) Empty() bool {
	return reflect.DeepEqual(c, &CheckType{})
}

func (c *CheckType) IsScript() bool {
	return c.Script != "" || len(c.ScriptArgs) > 0
}

func (c *CheckType) IsTTL() bool {
	return c.TTL > 0
}

func (c *CheckType) IsMonitor() bool {
	return c.IsScript() && c.DockerContainerID == "" && c.Interval > 0
}

func (c *CheckType) IsHTTP() bool {
	return c.HTTP != "" && c.Interval > 0
}

func (c *CheckType) IsTCP() bool {
	return c.TCP != "" && c.Interval > 0
}

func (c *CheckType) IsDocker() bool {
	return c.IsScript() && c.DockerContainerID != "" && c.Interval > 0
}

func (c *CheckType) IsGRPC() bool {
	return c.GRPC != "" && c.Interval > 0
}
