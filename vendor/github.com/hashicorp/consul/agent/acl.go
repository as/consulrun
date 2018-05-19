package agent

import (
	"fmt"
	"sync"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/config"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/types"
	"github.com/hashicorp/golang-lru"
	"github.com/hashicorp/serf/serf"
)

const (
	anonymousToken = "anonymous"

	aclCacheSize = 10 * 1024
)

type aclCacheEntry struct {
	ACL acl.ACL

	Expires time.Time

	ETag string
}

type aclManager struct {
	acls *lru.TwoQueueCache

	master acl.ACL

	down acl.ACL

	disabled     time.Time
	disabledLock sync.RWMutex
}

func newACLManager(config *config.RuntimeConfig) (*aclManager, error) {

	acls, err := lru.New2Q(aclCacheSize)
	if err != nil {
		return nil, err
	}

	policy := &acl.Policy{
		Agents: []*acl.AgentPolicy{
			{
				Node:   config.NodeName,
				Policy: acl.PolicyWrite,
			},
		},
		Nodes: []*acl.NodePolicy{
			{
				Name:   "",
				Policy: acl.PolicyRead,
			},
		},
	}
	master, err := acl.New(acl.DenyAll(), policy, nil)
	if err != nil {
		return nil, err
	}

	var down acl.ACL
	switch config.ACLDownPolicy {
	case "allow":
		down = acl.AllowAll()
	case "deny":
		down = acl.DenyAll()
	case "extend-cache":

	default:
		return nil, fmt.Errorf("invalid ACL down policy %q", config.ACLDownPolicy)
	}

	return &aclManager{
		acls:   acls,
		master: master,
		down:   down,
	}, nil
}

func (m *aclManager) isDisabled() bool {
	m.disabledLock.RLock()
	defer m.disabledLock.RUnlock()
	return time.Now().Before(m.disabled)
}

func (m *aclManager) lookupACL(a *Agent, id string) (acl.ACL, error) {

	if len(id) == 0 {
		id = anonymousToken
	} else if acl.RootACL(id) != nil {
		return nil, acl.ErrRootDenied
	} else if a.tokens.IsAgentMasterToken(id) {
		return m.master, nil
	}

	var cached *aclCacheEntry
	if raw, ok := m.acls.Get(id); ok {
		cached = raw.(*aclCacheEntry)
	}
	if cached != nil && time.Now().Before(cached.Expires) {
		metrics.IncrCounter([]string{"consul", "acl", "cache_hit"}, 1)
		metrics.IncrCounter([]string{"acl", "cache_hit"}, 1)
		return cached.ACL, nil
	}
	metrics.IncrCounter([]string{"consul", "acl", "cache_miss"}, 1)
	metrics.IncrCounter([]string{"acl", "cache_miss"}, 1)

	args := structs.ACLPolicyRequest{
		Datacenter: a.config.ACLDatacenter,
		ACL:        id,
	}
	if cached != nil {
		args.ETag = cached.ETag
	}
	var reply structs.ACLPolicy
	err := a.RPC("ACL.GetPolicy", &args, &reply)
	if err != nil {
		if acl.IsErrDisabled(err) {
			a.logger.Printf("[DEBUG] agent: ACLs disabled on servers, will check again after %s", a.config.ACLDisabledTTL)
			m.disabledLock.Lock()
			m.disabled = time.Now().Add(a.config.ACLDisabledTTL)
			m.disabledLock.Unlock()
			return nil, nil
		} else if acl.IsErrNotFound(err) {
			return nil, acl.ErrNotFound
		} else {
			a.logger.Printf("[DEBUG] agent: Failed to get policy for ACL from servers: %v", err)
			if m.down != nil {
				return m.down, nil
			} else if cached != nil {
				return cached.ACL, nil
			} else {
				return acl.DenyAll(), nil
			}
		}
	}

	var compiled acl.ACL
	if cached != nil && cached.ETag == reply.ETag {
		compiled = cached.ACL
	} else {
		parent := acl.RootACL(reply.Parent)
		if parent == nil {
			parent, err = m.lookupACL(a, reply.Parent)
			if err != nil {
				return nil, err
			}
		}

		acl, err := acl.New(parent, reply.Policy, nil)
		if err != nil {
			return nil, err
		}
		compiled = acl
	}

	cached = &aclCacheEntry{
		ACL:  compiled,
		ETag: reply.ETag,
	}
	if reply.TTL > 0 {
		cached.Expires = time.Now().Add(reply.TTL)
	}
	m.acls.Add(id, cached)
	return compiled, nil
}

func (a *Agent) resolveToken(id string) (acl.ACL, error) {

	if !a.config.ACLEnforceVersion8 {
		return nil, nil
	}

	if a.config.ACLDatacenter == "" {
		return nil, nil
	}

	if a.acls.isDisabled() {
		return nil, nil
	}

	return a.acls.lookupACL(a, id)
}

func (a *Agent) vetServiceRegister(token string, service *structs.NodeService) error {

	rule, err := a.resolveToken(token)
	if err != nil {
		return err
	}
	if rule == nil {
		return nil
	}

	if !rule.ServiceWrite(service.Service, nil) {
		return acl.ErrPermissionDenied
	}

	services := a.State.Services()
	if existing, ok := services[service.ID]; ok {
		if !rule.ServiceWrite(existing.Service, nil) {
			return acl.ErrPermissionDenied
		}
	}

	return nil
}

func (a *Agent) vetServiceUpdate(token string, serviceID string) error {

	rule, err := a.resolveToken(token)
	if err != nil {
		return err
	}
	if rule == nil {
		return nil
	}

	services := a.State.Services()
	if existing, ok := services[serviceID]; ok {
		if !rule.ServiceWrite(existing.Service, nil) {
			return acl.ErrPermissionDenied
		}
	} else {
		return fmt.Errorf("Unknown service %q", serviceID)
	}

	return nil
}

func (a *Agent) vetCheckRegister(token string, check *structs.HealthCheck) error {

	rule, err := a.resolveToken(token)
	if err != nil {
		return err
	}
	if rule == nil {
		return nil
	}

	if len(check.ServiceName) > 0 {
		if !rule.ServiceWrite(check.ServiceName, nil) {
			return acl.ErrPermissionDenied
		}
	} else {
		if !rule.NodeWrite(a.config.NodeName, nil) {
			return acl.ErrPermissionDenied
		}
	}

	checks := a.State.Checks()
	if existing, ok := checks[check.CheckID]; ok {
		if len(existing.ServiceName) > 0 {
			if !rule.ServiceWrite(existing.ServiceName, nil) {
				return acl.ErrPermissionDenied
			}
		} else {
			if !rule.NodeWrite(a.config.NodeName, nil) {
				return acl.ErrPermissionDenied
			}
		}
	}

	return nil
}

func (a *Agent) vetCheckUpdate(token string, checkID types.CheckID) error {

	rule, err := a.resolveToken(token)
	if err != nil {
		return err
	}
	if rule == nil {
		return nil
	}

	checks := a.State.Checks()
	if existing, ok := checks[checkID]; ok {
		if len(existing.ServiceName) > 0 {
			if !rule.ServiceWrite(existing.ServiceName, nil) {
				return acl.ErrPermissionDenied
			}
		} else {
			if !rule.NodeWrite(a.config.NodeName, nil) {
				return acl.ErrPermissionDenied
			}
		}
	} else {
		return fmt.Errorf("Unknown check %q", checkID)
	}

	return nil
}

func (a *Agent) filterMembers(token string, members *[]serf.Member) error {

	rule, err := a.resolveToken(token)
	if err != nil {
		return err
	}
	if rule == nil {
		return nil
	}

	m := *members
	for i := 0; i < len(m); i++ {
		node := m[i].Name
		if rule.NodeRead(node) {
			continue
		}
		a.logger.Printf("[DEBUG] agent: dropping node %q from result due to ACLs", node)
		m = append(m[:i], m[i+1:]...)
		i--
	}
	*members = m
	return nil
}

func (a *Agent) filterServices(token string, services *map[string]*structs.NodeService) error {

	rule, err := a.resolveToken(token)
	if err != nil {
		return err
	}
	if rule == nil {
		return nil
	}

	for id, service := range *services {
		if rule.ServiceRead(service.Service) {
			continue
		}
		a.logger.Printf("[DEBUG] agent: dropping service %q from result due to ACLs", id)
		delete(*services, id)
	}
	return nil
}

func (a *Agent) filterChecks(token string, checks *map[types.CheckID]*structs.HealthCheck) error {

	rule, err := a.resolveToken(token)
	if err != nil {
		return err
	}
	if rule == nil {
		return nil
	}

	for id, check := range *checks {
		if len(check.ServiceName) > 0 {
			if rule.ServiceRead(check.ServiceName) {
				continue
			}
		} else {
			if rule.NodeRead(a.config.NodeName) {
				continue
			}
		}
		a.logger.Printf("[DEBUG] agent: dropping check %q from result due to ACLs", id)
		delete(*checks, id)
	}
	return nil
}
