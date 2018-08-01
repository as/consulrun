package acl

import (
	"github.com/armon/go-radix"
	"github.com/as/consulrun/hashicorp/consul/sentinel"
)

var (
	allowAll ACL

	denyAll ACL

	manageAll ACL
)

const DefaultPolicyEnforcementLevel = "hard-mandatory"

func init() {

	allowAll = &StaticACL{
		allowManage:  false,
		defaultAllow: true,
	}
	denyAll = &StaticACL{
		allowManage:  false,
		defaultAllow: false,
	}
	manageAll = &StaticACL{
		allowManage:  true,
		defaultAllow: true,
	}
}

type ACL interface {
	ACLList() bool

	ACLModify() bool

	AgentRead(string) bool

	AgentWrite(string) bool

	EventRead(string) bool

	EventWrite(string) bool

	KeyList(string) bool

	KeyRead(string) bool

	KeyWrite(string, sentinel.ScopeFn) bool

	KeyWritePrefix(string) bool

	KeyringRead() bool

	KeyringWrite() bool

	NodeRead(string) bool

	NodeWrite(string, sentinel.ScopeFn) bool

	OperatorRead() bool

	OperatorWrite() bool

	PreparedQueryRead(string) bool

	PreparedQueryWrite(string) bool

	ServiceRead(string) bool

	ServiceWrite(string, sentinel.ScopeFn) bool

	SessionRead(string) bool

	SessionWrite(string) bool

	Snapshot() bool
}

type StaticACL struct {
	allowManage  bool
	defaultAllow bool
}

func (s *StaticACL) ACLList() bool {
	return s.allowManage
}

func (s *StaticACL) ACLModify() bool {
	return s.allowManage
}

func (s *StaticACL) AgentRead(string) bool {
	return s.defaultAllow
}

func (s *StaticACL) AgentWrite(string) bool {
	return s.defaultAllow
}

func (s *StaticACL) EventRead(string) bool {
	return s.defaultAllow
}

func (s *StaticACL) EventWrite(string) bool {
	return s.defaultAllow
}

func (s *StaticACL) KeyRead(string) bool {
	return s.defaultAllow
}

func (s *StaticACL) KeyList(string) bool {
	return s.defaultAllow
}

func (s *StaticACL) KeyWrite(string, sentinel.ScopeFn) bool {
	return s.defaultAllow
}

func (s *StaticACL) KeyWritePrefix(string) bool {
	return s.defaultAllow
}

func (s *StaticACL) KeyringRead() bool {
	return s.defaultAllow
}

func (s *StaticACL) KeyringWrite() bool {
	return s.defaultAllow
}

func (s *StaticACL) NodeRead(string) bool {
	return s.defaultAllow
}

func (s *StaticACL) NodeWrite(string, sentinel.ScopeFn) bool {
	return s.defaultAllow
}

func (s *StaticACL) OperatorRead() bool {
	return s.defaultAllow
}

func (s *StaticACL) OperatorWrite() bool {
	return s.defaultAllow
}

func (s *StaticACL) PreparedQueryRead(string) bool {
	return s.defaultAllow
}

func (s *StaticACL) PreparedQueryWrite(string) bool {
	return s.defaultAllow
}

func (s *StaticACL) ServiceRead(string) bool {
	return s.defaultAllow
}

func (s *StaticACL) ServiceWrite(string, sentinel.ScopeFn) bool {
	return s.defaultAllow
}

func (s *StaticACL) SessionRead(string) bool {
	return s.defaultAllow
}

func (s *StaticACL) SessionWrite(string) bool {
	return s.defaultAllow
}

func (s *StaticACL) Snapshot() bool {
	return s.allowManage
}

func AllowAll() ACL {
	return allowAll
}

func DenyAll() ACL {
	return denyAll
}

func ManageAll() ACL {
	return manageAll
}

func RootACL(id string) ACL {
	switch id {
	case "allow":
		return allowAll
	case "deny":
		return denyAll
	case "manage":
		return manageAll
	default:
		return nil
	}
}

type PolicyRule struct {
	aclPolicy string

	sentinelPolicy Sentinel
}

type PolicyACL struct {
	parent ACL

	sentinel sentinel.Evaluator

	agentRules *radix.Tree

	keyRules *radix.Tree

	nodeRules *radix.Tree

	serviceRules *radix.Tree

	sessionRules *radix.Tree

	eventRules *radix.Tree

	preparedQueryRules *radix.Tree

	keyringRule string

	operatorRule string
}

func New(parent ACL, policy *Policy, sentinel sentinel.Evaluator) (*PolicyACL, error) {
	p := &PolicyACL{
		parent:             parent,
		agentRules:         radix.New(),
		keyRules:           radix.New(),
		nodeRules:          radix.New(),
		serviceRules:       radix.New(),
		sessionRules:       radix.New(),
		eventRules:         radix.New(),
		preparedQueryRules: radix.New(),
		sentinel:           sentinel,
	}

	for _, ap := range policy.Agents {
		p.agentRules.Insert(ap.Node, ap.Policy)
	}

	for _, kp := range policy.Keys {
		policyRule := PolicyRule{
			aclPolicy:      kp.Policy,
			sentinelPolicy: kp.Sentinel,
		}
		p.keyRules.Insert(kp.Prefix, policyRule)
	}

	for _, np := range policy.Nodes {
		policyRule := PolicyRule{
			aclPolicy:      np.Policy,
			sentinelPolicy: np.Sentinel,
		}
		p.nodeRules.Insert(np.Name, policyRule)
	}

	for _, sp := range policy.Services {
		policyRule := PolicyRule{
			aclPolicy:      sp.Policy,
			sentinelPolicy: sp.Sentinel,
		}
		p.serviceRules.Insert(sp.Name, policyRule)
	}

	for _, sp := range policy.Sessions {
		p.sessionRules.Insert(sp.Node, sp.Policy)
	}

	for _, ep := range policy.Events {
		p.eventRules.Insert(ep.Event, ep.Policy)
	}

	for _, pq := range policy.PreparedQueries {
		p.preparedQueryRules.Insert(pq.Prefix, pq.Policy)
	}

	p.keyringRule = policy.Keyring

	p.operatorRule = policy.Operator

	return p, nil
}

func (p *PolicyACL) ACLList() bool {
	return p.parent.ACLList()
}

func (p *PolicyACL) ACLModify() bool {
	return p.parent.ACLModify()
}

func (p *PolicyACL) AgentRead(node string) bool {

	_, rule, ok := p.agentRules.LongestPrefix(node)

	if ok {
		switch rule {
		case PolicyRead, PolicyWrite:
			return true
		default:
			return false
		}
	}

	return p.parent.AgentRead(node)
}

func (p *PolicyACL) AgentWrite(node string) bool {

	_, rule, ok := p.agentRules.LongestPrefix(node)

	if ok {
		switch rule {
		case PolicyWrite:
			return true
		default:
			return false
		}
	}

	return p.parent.AgentWrite(node)
}

func (p *PolicyACL) Snapshot() bool {
	return p.parent.Snapshot()
}

func (p *PolicyACL) EventRead(name string) bool {

	if _, rule, ok := p.eventRules.LongestPrefix(name); ok {
		switch rule {
		case PolicyRead, PolicyWrite:
			return true
		default:
			return false
		}
	}

	return p.parent.EventRead(name)
}

func (p *PolicyACL) EventWrite(name string) bool {

	if _, rule, ok := p.eventRules.LongestPrefix(name); ok {
		return rule == PolicyWrite
	}

	return p.parent.EventWrite(name)
}

func (p *PolicyACL) KeyRead(key string) bool {

	_, rule, ok := p.keyRules.LongestPrefix(key)
	if ok {
		pr := rule.(PolicyRule)
		switch pr.aclPolicy {
		case PolicyRead, PolicyWrite, PolicyList:
			return true
		default:
			return false
		}
	}

	return p.parent.KeyRead(key)
}

func (p *PolicyACL) KeyList(key string) bool {

	_, rule, ok := p.keyRules.LongestPrefix(key)
	if ok {
		pr := rule.(PolicyRule)
		switch pr.aclPolicy {
		case PolicyList, PolicyWrite:
			return true
		default:
			return false
		}
	}

	return p.parent.KeyList(key)
}

func (p *PolicyACL) KeyWrite(key string, scope sentinel.ScopeFn) bool {

	_, rule, ok := p.keyRules.LongestPrefix(key)
	if ok {
		pr := rule.(PolicyRule)
		switch pr.aclPolicy {
		case PolicyWrite:
			return p.executeCodePolicy(&pr.sentinelPolicy, scope)
		default:
			return false
		}
	}

	return p.parent.KeyWrite(key, scope)
}

func (p *PolicyACL) KeyWritePrefix(prefix string) bool {

	_, rule, ok := p.keyRules.LongestPrefix(prefix)
	if ok && rule.(PolicyRule).aclPolicy != PolicyWrite {
		return false
	}

	deny := false
	p.keyRules.WalkPrefix(prefix, func(path string, rule interface{}) bool {

		if rule.(PolicyRule).aclPolicy != PolicyWrite {
			deny = true
			return true
		}
		return false
	})

	if deny {
		return false
	}

	if ok {
		return true
	}

	return p.parent.KeyWritePrefix(prefix)
}

func (p *PolicyACL) KeyringRead() bool {
	switch p.keyringRule {
	case PolicyRead, PolicyWrite:
		return true
	case PolicyDeny:
		return false
	default:
		return p.parent.KeyringRead()
	}
}

func (p *PolicyACL) KeyringWrite() bool {
	if p.keyringRule == PolicyWrite {
		return true
	}
	return p.parent.KeyringWrite()
}

func (p *PolicyACL) OperatorRead() bool {
	switch p.operatorRule {
	case PolicyRead, PolicyWrite:
		return true
	case PolicyDeny:
		return false
	default:
		return p.parent.OperatorRead()
	}
}

func (p *PolicyACL) NodeRead(name string) bool {

	_, rule, ok := p.nodeRules.LongestPrefix(name)

	if ok {
		pr := rule.(PolicyRule)
		switch pr.aclPolicy {
		case PolicyRead, PolicyWrite:
			return true
		default:
			return false
		}
	}

	return p.parent.NodeRead(name)
}

func (p *PolicyACL) NodeWrite(name string, scope sentinel.ScopeFn) bool {

	_, rule, ok := p.nodeRules.LongestPrefix(name)

	if ok {
		pr := rule.(PolicyRule)
		switch pr.aclPolicy {
		case PolicyWrite:
			return true
		default:
			return false
		}
	}

	return p.parent.NodeWrite(name, scope)
}

func (p *PolicyACL) OperatorWrite() bool {
	if p.operatorRule == PolicyWrite {
		return true
	}
	return p.parent.OperatorWrite()
}

func (p *PolicyACL) PreparedQueryRead(prefix string) bool {

	_, rule, ok := p.preparedQueryRules.LongestPrefix(prefix)

	if ok {
		switch rule {
		case PolicyRead, PolicyWrite:
			return true
		default:
			return false
		}
	}

	return p.parent.PreparedQueryRead(prefix)
}

func (p *PolicyACL) PreparedQueryWrite(prefix string) bool {

	_, rule, ok := p.preparedQueryRules.LongestPrefix(prefix)

	if ok {
		switch rule {
		case PolicyWrite:
			return true
		default:
			return false
		}
	}

	return p.parent.PreparedQueryWrite(prefix)
}

func (p *PolicyACL) ServiceRead(name string) bool {

	_, rule, ok := p.serviceRules.LongestPrefix(name)
	if ok {
		pr := rule.(PolicyRule)
		switch pr.aclPolicy {
		case PolicyRead, PolicyWrite:
			return true
		default:
			return false
		}
	}

	return p.parent.ServiceRead(name)
}

func (p *PolicyACL) ServiceWrite(name string, scope sentinel.ScopeFn) bool {

	_, rule, ok := p.serviceRules.LongestPrefix(name)
	if ok {
		pr := rule.(PolicyRule)
		switch pr.aclPolicy {
		case PolicyWrite:
			return true
		default:
			return false
		}
	}

	return p.parent.ServiceWrite(name, scope)
}

func (p *PolicyACL) SessionRead(node string) bool {

	_, rule, ok := p.sessionRules.LongestPrefix(node)

	if ok {
		switch rule {
		case PolicyRead, PolicyWrite:
			return true
		default:
			return false
		}
	}

	return p.parent.SessionRead(node)
}

func (p *PolicyACL) SessionWrite(node string) bool {

	_, rule, ok := p.sessionRules.LongestPrefix(node)

	if ok {
		switch rule {
		case PolicyWrite:
			return true
		default:
			return false
		}
	}

	return p.parent.SessionWrite(node)
}

func (p *PolicyACL) executeCodePolicy(policy *Sentinel, scope sentinel.ScopeFn) bool {
	if p.sentinel == nil {
		return true
	}

	if policy.Code == "" || scope == nil {
		return true
	}

	enforcement := policy.EnforcementLevel
	if enforcement == "" {
		enforcement = DefaultPolicyEnforcementLevel
	}

	return p.sentinel.Execute(policy.Code, enforcement, scope())
}
