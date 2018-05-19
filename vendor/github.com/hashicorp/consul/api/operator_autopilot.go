package api

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

type AutopilotConfiguration struct {
	CleanupDeadServers bool

	LastContactThreshold *ReadableDuration

	MaxTrailingLogs uint64

	ServerStabilizationTime *ReadableDuration

	RedundancyZoneTag string

	DisableUpgradeMigration bool

	UpgradeVersionTag string

	CreateIndex uint64

	ModifyIndex uint64
}

type ServerHealth struct {
	ID string

	Name string

	Address string

	SerfStatus string

	Version string

	Leader bool

	LastContact *ReadableDuration

	LastTerm uint64

	LastIndex uint64

	Healthy bool

	Voter bool

	StableSince time.Time
}

type OperatorHealthReply struct {
	Healthy bool

	FailureTolerance int

	Servers []ServerHealth
}

type ReadableDuration time.Duration

func NewReadableDuration(dur time.Duration) *ReadableDuration {
	d := ReadableDuration(dur)
	return &d
}

func (d *ReadableDuration) String() string {
	return d.Duration().String()
}

func (d *ReadableDuration) Duration() time.Duration {
	if d == nil {
		return time.Duration(0)
	}
	return time.Duration(*d)
}

func (d *ReadableDuration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, d.Duration().String())), nil
}

func (d *ReadableDuration) UnmarshalJSON(raw []byte) error {
	if d == nil {
		return fmt.Errorf("cannot unmarshal to nil pointer")
	}

	str := string(raw)
	if len(str) < 2 || str[0] != '"' || str[len(str)-1] != '"' {
		return fmt.Errorf("must be enclosed with quotes: %s", str)
	}
	dur, err := time.ParseDuration(str[1 : len(str)-1])
	if err != nil {
		return err
	}
	*d = ReadableDuration(dur)
	return nil
}

func (op *Operator) AutopilotGetConfiguration(q *QueryOptions) (*AutopilotConfiguration, error) {
	r := op.c.newRequest("GET", "/v1/operator/autopilot/configuration")
	r.setQueryOptions(q)
	_, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out AutopilotConfiguration
	if err := decodeBody(resp, &out); err != nil {
		return nil, err
	}

	return &out, nil
}

func (op *Operator) AutopilotSetConfiguration(conf *AutopilotConfiguration, q *WriteOptions) error {
	r := op.c.newRequest("PUT", "/v1/operator/autopilot/configuration")
	r.setWriteOptions(q)
	r.obj = conf
	_, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (op *Operator) AutopilotCASConfiguration(conf *AutopilotConfiguration, q *WriteOptions) (bool, error) {
	r := op.c.newRequest("PUT", "/v1/operator/autopilot/configuration")
	r.setWriteOptions(q)
	r.params.Set("cas", strconv.FormatUint(conf.ModifyIndex, 10))
	r.obj = conf
	_, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		return false, fmt.Errorf("Failed to read response: %v", err)
	}
	res := strings.Contains(buf.String(), "true")

	return res, nil
}

func (op *Operator) AutopilotServerHealth(q *QueryOptions) (*OperatorHealthReply, error) {
	r := op.c.newRequest("GET", "/v1/operator/autopilot/health")
	r.setQueryOptions(q)
	_, resp, err := requireOK(op.c.doRequest(r))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out OperatorHealthReply
	if err := decodeBody(resp, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
