package acl

import (
	"fmt"

	"github.com/as/consulrun/hashicorp/consul/sentinel"
	"github.com/as/consulrun/hashicorp/hcl"
)

const (
	PolicyDeny  = "deny"
	PolicyRead  = "read"
	PolicyWrite = "write"
	PolicyList  = "list"
)

type Policy struct {
	ID              string                 `hcl:"-"`
	Agents          []*AgentPolicy         `hcl:"agent,expand"`
	Keys            []*KeyPolicy           `hcl:"key,expand"`
	Nodes           []*NodePolicy          `hcl:"node,expand"`
	Services        []*ServicePolicy       `hcl:"service,expand"`
	Sessions        []*SessionPolicy       `hcl:"session,expand"`
	Events          []*EventPolicy         `hcl:"event,expand"`
	PreparedQueries []*PreparedQueryPolicy `hcl:"query,expand"`
	Keyring         string                 `hcl:"keyring"`
	Operator        string                 `hcl:"operator"`
}

type Sentinel struct {
	Code             string
	EnforcementLevel string
}

type AgentPolicy struct {
	Node   string `hcl:",key"`
	Policy string
}

func (a *AgentPolicy) GoString() string {
	return fmt.Sprintf("%#v", *a)
}

type KeyPolicy struct {
	Prefix   string `hcl:",key"`
	Policy   string
	Sentinel Sentinel
}

func (k *KeyPolicy) GoString() string {
	return fmt.Sprintf("%#v", *k)
}

type NodePolicy struct {
	Name     string `hcl:",key"`
	Policy   string
	Sentinel Sentinel
}

func (n *NodePolicy) GoString() string {
	return fmt.Sprintf("%#v", *n)
}

type ServicePolicy struct {
	Name     string `hcl:",key"`
	Policy   string
	Sentinel Sentinel
}

func (s *ServicePolicy) GoString() string {
	return fmt.Sprintf("%#v", *s)
}

type SessionPolicy struct {
	Node   string `hcl:",key"`
	Policy string
}

func (s *SessionPolicy) GoString() string {
	return fmt.Sprintf("%#v", *s)
}

type EventPolicy struct {
	Event  string `hcl:",key"`
	Policy string
}

func (e *EventPolicy) GoString() string {
	return fmt.Sprintf("%#v", *e)
}

type PreparedQueryPolicy struct {
	Prefix string `hcl:",key"`
	Policy string
}

func (p *PreparedQueryPolicy) GoString() string {
	return fmt.Sprintf("%#v", *p)
}

func isPolicyValid(policy string) bool {
	switch policy {
	case PolicyDeny:
		return true
	case PolicyRead:
		return true
	case PolicyWrite:
		return true
	default:
		return false
	}
}

func isSentinelValid(sentinel sentinel.Evaluator, basicPolicy string, sp Sentinel) error {

	if sentinel == nil {
		return nil
	}
	if sp.Code == "" {
		return nil
	}

	if basicPolicy != PolicyWrite {
		return fmt.Errorf("code is only allowed for write policies")
	}

	switch sp.EnforcementLevel {
	case "", "soft-mandatory", "hard-mandatory":

	default:
		return fmt.Errorf("unsupported enforcement level %q", sp.EnforcementLevel)
	}
	return sentinel.Compile(sp.Code)
}

func Parse(rules string, sentinel sentinel.Evaluator) (*Policy, error) {

	p := &Policy{}
	if rules == "" {

		return p, nil
	}

	if err := hcl.Decode(p, rules); err != nil {
		return nil, fmt.Errorf("Failed to parse ACL rules: %v", err)
	}

	for _, ap := range p.Agents {
		if !isPolicyValid(ap.Policy) {
			return nil, fmt.Errorf("Invalid agent policy: %#v", ap)
		}
	}

	for _, kp := range p.Keys {
		if kp.Policy != PolicyList && !isPolicyValid(kp.Policy) {
			return nil, fmt.Errorf("Invalid key policy: %#v", kp)
		}
		if err := isSentinelValid(sentinel, kp.Policy, kp.Sentinel); err != nil {
			return nil, fmt.Errorf("Invalid key Sentinel policy: %#v, got error:%v", kp, err)
		}
	}

	for _, np := range p.Nodes {
		if !isPolicyValid(np.Policy) {
			return nil, fmt.Errorf("Invalid node policy: %#v", np)
		}
		if err := isSentinelValid(sentinel, np.Policy, np.Sentinel); err != nil {
			return nil, fmt.Errorf("Invalid node Sentinel policy: %#v, got error:%v", np, err)
		}
	}

	for _, sp := range p.Services {
		if !isPolicyValid(sp.Policy) {
			return nil, fmt.Errorf("Invalid service policy: %#v", sp)
		}
		if err := isSentinelValid(sentinel, sp.Policy, sp.Sentinel); err != nil {
			return nil, fmt.Errorf("Invalid service Sentinel policy: %#v, got error:%v", sp, err)
		}
	}

	for _, sp := range p.Sessions {
		if !isPolicyValid(sp.Policy) {
			return nil, fmt.Errorf("Invalid session policy: %#v", sp)
		}
	}

	for _, ep := range p.Events {
		if !isPolicyValid(ep.Policy) {
			return nil, fmt.Errorf("Invalid event policy: %#v", ep)
		}
	}

	for _, pq := range p.PreparedQueries {
		if !isPolicyValid(pq.Policy) {
			return nil, fmt.Errorf("Invalid query policy: %#v", pq)
		}
	}

	if p.Keyring != "" && !isPolicyValid(p.Keyring) {
		return nil, fmt.Errorf("Invalid keyring policy: %#v", p.Keyring)
	}

	if p.Operator != "" && !isPolicyValid(p.Operator) {
		return nil, fmt.Errorf("Invalid operator policy: %#v", p.Operator)
	}

	return p, nil
}
