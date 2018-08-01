package consul

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/armon/go-metrics"
	"github.com/as/consulrun/hashicorp/consul/acl"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/api"
	"github.com/as/consulrun/hashicorp/consul/sentinel"
	"github.com/as/consulrun/hashicorp/golang-lru"
)

const (
	anonymousToken = "anonymous"

	redactedToken = "<hidden>"

	aclCacheSize = 10 * 1024
)

type aclCacheEntry struct {
	ACL     acl.ACL
	Expires time.Time
	ETag    string
}

func (s *Server) aclLocalFault(id string) (string, string, error) {
	defer metrics.MeasureSince([]string{"consul", "acl", "fault"}, time.Now())
	defer metrics.MeasureSince([]string{"acl", "fault"}, time.Now())

	state := s.fsm.State()
	_, rule, err := state.ACLGet(nil, id)
	if err != nil {
		return "", "", err
	}
	if rule == nil {
		return "", "", acl.ErrNotFound
	}

	if rule.Type == structs.ACLTypeManagement {
		return "manage", "", nil
	}

	return s.config.ACLDefaultPolicy, rule.Rules, nil
}

func (s *Server) resolveToken(id string) (acl.ACL, error) {

	authDC := s.config.ACLDatacenter
	if len(authDC) == 0 {
		return nil, nil
	}
	defer metrics.MeasureSince([]string{"consul", "acl", "resolveToken"}, time.Now())
	defer metrics.MeasureSince([]string{"acl", "resolveToken"}, time.Now())

	if len(id) == 0 {
		id = anonymousToken
	} else if acl.RootACL(id) != nil {
		return nil, acl.ErrRootDenied
	}

	if s.config.Datacenter == authDC && s.IsLeader() {
		return s.aclAuthCache.GetACL(id)
	}

	return s.aclCache.lookupACL(id, authDC)
}

type rpcFn func(string, interface{}, interface{}) error

type aclCache struct {
	config *Config
	logger *log.Logger

	acls *lru.TwoQueueCache

	sentinel sentinel.Evaluator

	policies *lru.TwoQueueCache

	rpc rpcFn

	local acl.FaultFunc
}

func newACLCache(conf *Config, logger *log.Logger, rpc rpcFn, local acl.FaultFunc, sentinel sentinel.Evaluator) (*aclCache, error) {
	var err error
	cache := &aclCache{
		config:   conf,
		logger:   logger,
		rpc:      rpc,
		local:    local,
		sentinel: sentinel,
	}

	cache.acls, err = lru.New2Q(aclCacheSize)
	if err != nil {
		return nil, fmt.Errorf("Failed to create ACL cache: %v", err)
	}

	cache.policies, err = lru.New2Q(aclCacheSize)
	if err != nil {
		return nil, fmt.Errorf("Failed to create ACL policy cache: %v", err)
	}

	return cache, nil
}

func (c *aclCache) lookupACL(id, authDC string) (acl.ACL, error) {

	var cached *aclCacheEntry
	raw, ok := c.acls.Get(id)
	if ok {
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
		Datacenter: authDC,
		ACL:        id,
	}
	if cached != nil {
		args.ETag = cached.ETag
	}
	var reply structs.ACLPolicy
	err := c.rpc("ACL.GetPolicy", &args, &reply)
	if err == nil {
		return c.useACLPolicy(id, authDC, cached, &reply)
	}

	if acl.IsErrNotFound(err) {
		return nil, acl.ErrNotFound
	}
	c.logger.Printf("[ERR] consul.acl: Failed to get policy from ACL datacenter: %v", err)

	if c.local != nil && c.config.ACLDownPolicy == "extend-cache" {
		parent, rules, err := c.local(id)
		if err != nil {

			c.logger.Printf("[DEBUG] consul.acl: Failed to get policy from replicated ACLs: %v", err)
			goto ACL_DOWN
		}

		policy, err := acl.Parse(rules, c.sentinel)
		if err != nil {
			c.logger.Printf("[DEBUG] consul.acl: Failed to parse policy for replicated ACL: %v", err)
			goto ACL_DOWN
		}
		policy.ID = acl.RuleID(rules)

		metrics.IncrCounter([]string{"consul", "acl", "replication_hit"}, 1)
		metrics.IncrCounter([]string{"acl", "replication_hit"}, 1)
		reply.ETag = makeACLETag(parent, policy)
		reply.TTL = c.config.ACLTTL
		reply.Parent = parent
		reply.Policy = policy
		return c.useACLPolicy(id, authDC, cached, &reply)
	}

ACL_DOWN:

	switch c.config.ACLDownPolicy {
	case "allow":
		return acl.AllowAll(), nil
	case "extend-cache":
		if cached != nil {
			return cached.ACL, nil
		}
		fallthrough
	default:
		return acl.DenyAll(), nil
	}
}

func (c *aclCache) useACLPolicy(id, authDC string, cached *aclCacheEntry, p *structs.ACLPolicy) (acl.ACL, error) {

	if cached != nil && cached.ETag == p.ETag {
		if p.TTL > 0 {

			cached.Expires = time.Now().Add(p.TTL)
		}
		return cached.ACL, nil
	}

	var compiled acl.ACL
	raw, ok := c.policies.Get(p.ETag)
	if ok {
		compiled = raw.(acl.ACL)
	} else {

		parent := acl.RootACL(p.Parent)
		if parent == nil {
			var err error
			parent, err = c.lookupACL(p.Parent, authDC)
			if err != nil {
				return nil, err
			}
		}

		acl, err := acl.New(parent, p.Policy, c.sentinel)
		if err != nil {
			return nil, err
		}

		c.policies.Add(p.ETag, acl)
		compiled = acl
	}

	cached = &aclCacheEntry{
		ACL:  compiled,
		ETag: p.ETag,
	}
	if p.TTL > 0 {
		cached.Expires = time.Now().Add(p.TTL)
	}
	c.acls.Add(id, cached)
	return compiled, nil
}

type aclFilter struct {
	acl             acl.ACL
	logger          *log.Logger
	enforceVersion8 bool
}

func newACLFilter(acl acl.ACL, logger *log.Logger, enforceVersion8 bool) *aclFilter {
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	return &aclFilter{
		acl:             acl,
		logger:          logger,
		enforceVersion8: enforceVersion8,
	}
}

func (f *aclFilter) allowNode(node string) bool {
	if !f.enforceVersion8 {
		return true
	}
	return f.acl.NodeRead(node)
}

func (f *aclFilter) allowService(service string) bool {
	if service == "" {
		return true
	}

	if !f.enforceVersion8 && service == structs.ConsulServiceID {
		return true
	}
	return f.acl.ServiceRead(service)
}

func (f *aclFilter) allowSession(node string) bool {
	if !f.enforceVersion8 {
		return true
	}
	return f.acl.SessionRead(node)
}

func (f *aclFilter) filterHealthChecks(checks *structs.HealthChecks) {
	hc := *checks
	for i := 0; i < len(hc); i++ {
		check := hc[i]
		if f.allowNode(check.Node) && f.allowService(check.ServiceName) {
			continue
		}
		f.logger.Printf("[DEBUG] consul: dropping check %q from result due to ACLs", check.CheckID)
		hc = append(hc[:i], hc[i+1:]...)
		i--
	}
	*checks = hc
}

func (f *aclFilter) filterServices(services structs.Services) {
	for svc := range services {
		if f.allowService(svc) {
			continue
		}
		f.logger.Printf("[DEBUG] consul: dropping service %q from result due to ACLs", svc)
		delete(services, svc)
	}
}

func (f *aclFilter) filterServiceNodes(nodes *structs.ServiceNodes) {
	sn := *nodes
	for i := 0; i < len(sn); i++ {
		node := sn[i]
		if f.allowNode(node.Node) && f.allowService(node.ServiceName) {
			continue
		}
		f.logger.Printf("[DEBUG] consul: dropping node %q from result due to ACLs", node.Node)
		sn = append(sn[:i], sn[i+1:]...)
		i--
	}
	*nodes = sn
}

func (f *aclFilter) filterNodeServices(services **structs.NodeServices) {
	if *services == nil {
		return
	}

	if !f.allowNode((*services).Node.Node) {
		*services = nil
		return
	}

	for svc := range (*services).Services {
		if f.allowService(svc) {
			continue
		}
		f.logger.Printf("[DEBUG] consul: dropping service %q from result due to ACLs", svc)
		delete((*services).Services, svc)
	}
}

func (f *aclFilter) filterCheckServiceNodes(nodes *structs.CheckServiceNodes) {
	csn := *nodes
	for i := 0; i < len(csn); i++ {
		node := csn[i]
		if f.allowNode(node.Node.Node) && f.allowService(node.Service.Service) {
			continue
		}
		f.logger.Printf("[DEBUG] consul: dropping node %q from result due to ACLs", node.Node.Node)
		csn = append(csn[:i], csn[i+1:]...)
		i--
	}
	*nodes = csn
}

func (f *aclFilter) filterSessions(sessions *structs.Sessions) {
	s := *sessions
	for i := 0; i < len(s); i++ {
		session := s[i]
		if f.allowSession(session.Node) {
			continue
		}
		f.logger.Printf("[DEBUG] consul: dropping session %q from result due to ACLs", session.ID)
		s = append(s[:i], s[i+1:]...)
		i--
	}
	*sessions = s
}

func (f *aclFilter) filterCoordinates(coords *structs.Coordinates) {
	c := *coords
	for i := 0; i < len(c); i++ {
		node := c[i].Node
		if f.allowNode(node) {
			continue
		}
		f.logger.Printf("[DEBUG] consul: dropping node %q from result due to ACLs", node)
		c = append(c[:i], c[i+1:]...)
		i--
	}
	*coords = c
}

func (f *aclFilter) filterNodeDump(dump *structs.NodeDump) {
	nd := *dump
	for i := 0; i < len(nd); i++ {
		info := nd[i]

		if node := info.Node; !f.allowNode(node) {
			f.logger.Printf("[DEBUG] consul: dropping node %q from result due to ACLs", node)
			nd = append(nd[:i], nd[i+1:]...)
			i--
			continue
		}

		for j := 0; j < len(info.Services); j++ {
			svc := info.Services[j].Service
			if f.allowService(svc) {
				continue
			}
			f.logger.Printf("[DEBUG] consul: dropping service %q from result due to ACLs", svc)
			info.Services = append(info.Services[:j], info.Services[j+1:]...)
			j--
		}

		for j := 0; j < len(info.Checks); j++ {
			chk := info.Checks[j]
			if f.allowService(chk.ServiceName) {
				continue
			}
			f.logger.Printf("[DEBUG] consul: dropping check %q from result due to ACLs", chk.CheckID)
			info.Checks = append(info.Checks[:j], info.Checks[j+1:]...)
			j--
		}
	}
	*dump = nd
}

func (f *aclFilter) filterNodes(nodes *structs.Nodes) {
	n := *nodes
	for i := 0; i < len(n); i++ {
		node := n[i].Node
		if f.allowNode(node) {
			continue
		}
		f.logger.Printf("[DEBUG] consul: dropping node %q from result due to ACLs", node)
		n = append(n[:i], n[i+1:]...)
		i--
	}
	*nodes = n
}

func (f *aclFilter) redactPreparedQueryTokens(query **structs.PreparedQuery) {

	if f.acl.ACLList() {
		return
	}

	if (*query).Token != "" {

		clone := *(*query)
		clone.Token = redactedToken
		*query = &clone
	}
}

func (f *aclFilter) filterPreparedQueries(queries *structs.PreparedQueries) {

	if f.acl.ACLList() {
		return
	}

	ret := make(structs.PreparedQueries, 0, len(*queries))
	for _, query := range *queries {

		prefix, ok := query.GetACLPrefix()
		if !ok || !f.acl.PreparedQueryRead(prefix) {
			f.logger.Printf("[DEBUG] consul: dropping prepared query %q from result due to ACLs", query.ID)
			continue
		}

		final := query
		f.redactPreparedQueryTokens(&final)
		ret = append(ret, final)
	}
	*queries = ret
}

func (s *Server) filterACL(token string, subj interface{}) error {

	acl, err := s.resolveToken(token)
	if err != nil {
		return err
	}

	if acl == nil {
		return nil
	}

	filt := newACLFilter(acl, s.logger, s.config.ACLEnforceVersion8)

	switch v := subj.(type) {
	case *structs.CheckServiceNodes:
		filt.filterCheckServiceNodes(v)

	case *structs.IndexedCheckServiceNodes:
		filt.filterCheckServiceNodes(&v.Nodes)

	case *structs.IndexedCoordinates:
		filt.filterCoordinates(&v.Coordinates)

	case *structs.IndexedHealthChecks:
		filt.filterHealthChecks(&v.HealthChecks)

	case *structs.IndexedNodeDump:
		filt.filterNodeDump(&v.Dump)

	case *structs.IndexedNodes:
		filt.filterNodes(&v.Nodes)

	case *structs.IndexedNodeServices:
		filt.filterNodeServices(&v.NodeServices)

	case *structs.IndexedServiceNodes:
		filt.filterServiceNodes(&v.ServiceNodes)

	case *structs.IndexedServices:
		filt.filterServices(v.Services)

	case *structs.IndexedSessions:
		filt.filterSessions(&v.Sessions)

	case *structs.IndexedPreparedQueries:
		filt.filterPreparedQueries(&v.Queries)

	case **structs.PreparedQuery:
		filt.redactPreparedQueryTokens(v)

	default:
		panic(fmt.Errorf("Unhandled type passed to ACL filter: %#v", subj))
	}

	return nil
}

//
func vetRegisterWithACL(rule acl.ACL, subj *structs.RegisterRequest,
	ns *structs.NodeServices) error {

	if rule == nil {
		return nil
	}

	var memo map[string]interface{}
	scope := func() map[string]interface{} {
		if memo != nil {
			return memo
		}

		node := &api.Node{
			ID:              string(subj.ID),
			Node:            subj.Node,
			Address:         subj.Address,
			Datacenter:      subj.Datacenter,
			TaggedAddresses: subj.TaggedAddresses,
			Meta:            subj.NodeMeta,
		}

		var service *api.AgentService
		if subj.Service != nil {
			service = &api.AgentService{
				ID:                subj.Service.ID,
				Service:           subj.Service.Service,
				Tags:              subj.Service.Tags,
				Meta:              subj.Service.Meta,
				Address:           subj.Service.Address,
				Port:              subj.Service.Port,
				EnableTagOverride: subj.Service.EnableTagOverride,
			}
		}

		memo = sentinel.ScopeCatalogUpsert(node, service)
		return memo
	}

	needsNode := ns == nil || subj.ChangesNode(ns.Node)

	if needsNode && !rule.NodeWrite(subj.Node, scope) {
		return acl.ErrPermissionDenied
	}

	if subj.Service != nil {
		if !rule.ServiceWrite(subj.Service.Service, scope) {
			return acl.ErrPermissionDenied
		}

		if ns != nil {
			other, ok := ns.Services[subj.Service.ID]

			if ok && !rule.ServiceWrite(other.Service, nil) {
				return acl.ErrPermissionDenied
			}
		}
	}

	if subj.Check != nil {
		return fmt.Errorf("check member must be nil")
	}

	for _, check := range subj.Checks {

		if check.Node != subj.Node {
			return fmt.Errorf("Node '%s' for check '%s' doesn't match register request node '%s'",
				check.Node, check.CheckID, subj.Node)
		}

		if check.ServiceID == "" {
			if !rule.NodeWrite(subj.Node, scope) {
				return acl.ErrPermissionDenied
			}
			continue
		}

		if subj.Service != nil && subj.Service.ID == check.ServiceID {
			continue
		}

		if ns == nil {
			return fmt.Errorf("Unknown service '%s' for check '%s'", check.ServiceID, check.CheckID)
		}

		other, ok := ns.Services[check.ServiceID]
		if !ok {
			return fmt.Errorf("Unknown service '%s' for check '%s'", check.ServiceID, check.CheckID)
		}

		if !rule.ServiceWrite(other.Service, nil) {
			return acl.ErrPermissionDenied
		}
	}

	return nil
}

func vetDeregisterWithACL(rule acl.ACL, subj *structs.DeregisterRequest,
	ns *structs.NodeService, nc *structs.HealthCheck) error {

	if rule == nil {
		return nil
	}

	if subj.ServiceID != "" {
		if ns == nil {
			return fmt.Errorf("Unknown service '%s'", subj.ServiceID)
		}
		if !rule.ServiceWrite(ns.Service, nil) {
			return acl.ErrPermissionDenied
		}
	} else if subj.CheckID != "" {
		if nc == nil {
			return fmt.Errorf("Unknown check '%s'", subj.CheckID)
		}
		if nc.ServiceID != "" {
			if !rule.ServiceWrite(nc.ServiceName, nil) {
				return acl.ErrPermissionDenied
			}
		} else {
			if !rule.NodeWrite(subj.Node, nil) {
				return acl.ErrPermissionDenied
			}
		}
	} else {
		if !rule.NodeWrite(subj.Node, nil) {
			return acl.ErrPermissionDenied
		}
	}

	return nil
}
