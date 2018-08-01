package api

import (
	"fmt"
	"strings"
)

const (
	HealthAny      = "any"
	HealthPassing  = "passing"
	HealthWarning  = "warning"
	HealthCritical = "critical"
	HealthMaint    = "maintenance"
)

const (
	NodeMaint = "_node_maintenance"

	ServiceMaintPrefix = "_service_maintenance:"
)

type HealthCheck struct {
	Node        string
	CheckID     string
	Name        string
	Status      string
	Notes       string
	Output      string
	ServiceID   string
	ServiceName string
	ServiceTags []string

	Definition HealthCheckDefinition
}

type HealthCheckDefinition struct {
	HTTP                           string
	Header                         map[string][]string
	Method                         string
	TLSSkipVerify                  bool
	TCP                            string
	Interval                       ReadableDuration
	Timeout                        ReadableDuration
	DeregisterCriticalServiceAfter ReadableDuration
}

type HealthChecks []*HealthCheck

//
//
func (c HealthChecks) AggregatedStatus() string {
	var passing, warning, critical, maintenance bool
	for _, check := range c {
		id := string(check.CheckID)
		if id == NodeMaint || strings.HasPrefix(id, ServiceMaintPrefix) {
			maintenance = true
			continue
		}

		switch check.Status {
		case HealthPassing:
			passing = true
		case HealthWarning:
			warning = true
		case HealthCritical:
			critical = true
		default:
			return ""
		}
	}

	switch {
	case maintenance:
		return HealthMaint
	case critical:
		return HealthCritical
	case warning:
		return HealthWarning
	case passing:
		return HealthPassing
	default:
		return HealthPassing
	}
}

type ServiceEntry struct {
	Node    *Node
	Service *AgentService
	Checks  HealthChecks
}

type Health struct {
	c *Client
}

func (c *Client) Health() *Health {
	return &Health{c}
}

func (h *Health) Node(node string, q *QueryOptions) (HealthChecks, *QueryMeta, error) {
	r := h.c.newRequest("GET", "/v1/health/node/"+node)
	r.setQueryOptions(q)
	rtt, resp, err := requireOK(h.c.doRequest(r))
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	qm := &QueryMeta{}
	parseQueryMeta(resp, qm)
	qm.RequestTime = rtt

	var out HealthChecks
	if err := decodeBody(resp, &out); err != nil {
		return nil, nil, err
	}
	return out, qm, nil
}

func (h *Health) Checks(service string, q *QueryOptions) (HealthChecks, *QueryMeta, error) {
	r := h.c.newRequest("GET", "/v1/health/checks/"+service)
	r.setQueryOptions(q)
	rtt, resp, err := requireOK(h.c.doRequest(r))
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	qm := &QueryMeta{}
	parseQueryMeta(resp, qm)
	qm.RequestTime = rtt

	var out HealthChecks
	if err := decodeBody(resp, &out); err != nil {
		return nil, nil, err
	}
	return out, qm, nil
}

func (h *Health) Service(service, tag string, passingOnly bool, q *QueryOptions) ([]*ServiceEntry, *QueryMeta, error) {
	r := h.c.newRequest("GET", "/v1/health/service/"+service)
	r.setQueryOptions(q)
	if tag != "" {
		r.params.Set("tag", tag)
	}
	if passingOnly {
		r.params.Set(HealthPassing, "1")
	}
	rtt, resp, err := requireOK(h.c.doRequest(r))
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	qm := &QueryMeta{}
	parseQueryMeta(resp, qm)
	qm.RequestTime = rtt

	var out []*ServiceEntry
	if err := decodeBody(resp, &out); err != nil {
		return nil, nil, err
	}
	return out, qm, nil
}

func (h *Health) State(state string, q *QueryOptions) (HealthChecks, *QueryMeta, error) {
	switch state {
	case HealthAny:
	case HealthWarning:
	case HealthCritical:
	case HealthPassing:
	default:
		return nil, nil, fmt.Errorf("Unsupported state: %v", state)
	}
	r := h.c.newRequest("GET", "/v1/health/state/"+state)
	r.setQueryOptions(q)
	rtt, resp, err := requireOK(h.c.doRequest(r))
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	qm := &QueryMeta{}
	parseQueryMeta(resp, qm)
	qm.RequestTime = rtt

	var out HealthChecks
	if err := decodeBody(resp, &out); err != nil {
		return nil, nil, err
	}
	return out, qm, nil
}
