package consul

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/armon/go-metrics"
	"github.com/as/consulrun/hashicorp/consul/acl"
	"github.com/as/consulrun/hashicorp/consul/agent/consul/state"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/go-memdb"
	"github.com/as/consulrun/hashicorp/go-uuid"
)

var (
	ErrQueryNotFound = errors.New("Query not found")
)

type PreparedQuery struct {
	srv *Server
}

func (p *PreparedQuery) Apply(args *structs.PreparedQueryRequest, reply *string) (err error) {
	if done, err := p.srv.forward("PreparedQuery.Apply", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"consul", "prepared-query", "apply"}, time.Now())
	defer metrics.MeasureSince([]string{"prepared-query", "apply"}, time.Now())

	if args.Op == structs.PreparedQueryCreate {
		if args.Query.ID != "" {
			return fmt.Errorf("ID must be empty when creating a new prepared query")
		}

		state := p.srv.fsm.State()
		for {
			if args.Query.ID, err = uuid.GenerateUUID(); err != nil {
				return fmt.Errorf("UUID generation for prepared query failed: %v", err)
			}
			_, query, err := state.PreparedQueryGet(nil, args.Query.ID)
			if err != nil {
				return fmt.Errorf("Prepared query lookup failed: %v", err)
			}
			if query == nil {
				break
			}
		}
	}
	*reply = args.Query.ID

	rule, err := p.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}

	if prefix, ok := args.Query.GetACLPrefix(); ok {
		if rule != nil && !rule.PreparedQueryWrite(prefix) {
			p.srv.logger.Printf("[WARN] consul.prepared_query: Operation on prepared query '%s' denied due to ACLs", args.Query.ID)
			return acl.ErrPermissionDenied
		}
	}

	if args.Op != structs.PreparedQueryCreate {
		state := p.srv.fsm.State()
		_, query, err := state.PreparedQueryGet(nil, args.Query.ID)
		if err != nil {
			return fmt.Errorf("Prepared Query lookup failed: %v", err)
		}
		if query == nil {
			return fmt.Errorf("Cannot modify non-existent prepared query: '%s'", args.Query.ID)
		}

		if prefix, ok := query.GetACLPrefix(); ok {
			if rule != nil && !rule.PreparedQueryWrite(prefix) {
				p.srv.logger.Printf("[WARN] consul.prepared_query: Operation on prepared query '%s' denied due to ACLs", args.Query.ID)
				return acl.ErrPermissionDenied
			}
		}
	}

	switch args.Op {
	case structs.PreparedQueryCreate, structs.PreparedQueryUpdate:
		if err := parseQuery(args.Query, p.srv.config.ACLEnforceVersion8); err != nil {
			return fmt.Errorf("Invalid prepared query: %v", err)
		}

	case structs.PreparedQueryDelete:

	default:
		return fmt.Errorf("Unknown prepared query operation: %s", args.Op)
	}

	resp, err := p.srv.raftApply(structs.PreparedQueryRequestType, args)
	if err != nil {
		p.srv.logger.Printf("[ERR] consul.prepared_query: Apply failed %v", err)
		return err
	}
	if respErr, ok := resp.(error); ok {
		return respErr
	}

	return nil
}

func parseQuery(query *structs.PreparedQuery, enforceVersion8 bool) error {

	if enforceVersion8 {
		if query.Name == "" && query.Template.Type == "" && query.Session == "" {
			return fmt.Errorf("Must be bound to a session")
		}
	}

	if query.Token == redactedToken {
		return fmt.Errorf("Bad Token '%s', it looks like a query definition with a redacted token was submitted", query.Token)
	}

	if err := parseService(&query.Service); err != nil {
		return err
	}

	if err := parseDNS(&query.DNS); err != nil {
		return err
	}

	return nil
}

func parseService(svc *structs.ServiceQuery) error {

	if svc.Service == "" {
		return fmt.Errorf("Must provide a Service name to query")
	}

	if svc.Failover.NearestN < 0 {
		return fmt.Errorf("Bad NearestN '%d', must be >= 0", svc.Failover.NearestN)
	}

	if err := structs.ValidateMetadata(svc.NodeMeta, true); err != nil {
		return err
	}

	return nil
}

func parseDNS(dns *structs.QueryDNSOptions) error {
	if dns.TTL != "" {
		ttl, err := time.ParseDuration(dns.TTL)
		if err != nil {
			return fmt.Errorf("Bad DNS TTL '%s': %v", dns.TTL, err)
		}

		if ttl < 0 {
			return fmt.Errorf("DNS TTL '%d', must be >=0", ttl)
		}
	}

	return nil
}

func (p *PreparedQuery) Get(args *structs.PreparedQuerySpecificRequest,
	reply *structs.IndexedPreparedQueries) error {
	if done, err := p.srv.forward("PreparedQuery.Get", args, args, reply); done {
		return err
	}

	return p.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, query, err := state.PreparedQueryGet(ws, args.QueryID)
			if err != nil {
				return err
			}
			if query == nil {
				return ErrQueryNotFound
			}

			reply.Index = index
			reply.Queries = structs.PreparedQueries{query}
			if _, ok := query.GetACLPrefix(); !ok {
				return p.srv.filterACL(args.Token, &reply.Queries[0])
			}

			if err := p.srv.filterACL(args.Token, reply); err != nil {
				return err
			}

			if len(reply.Queries) == 0 {
				p.srv.logger.Printf("[WARN] consul.prepared_query: Request to get prepared query '%s' denied due to ACLs", args.QueryID)
				return acl.ErrPermissionDenied
			}

			return nil
		})
}

func (p *PreparedQuery) List(args *structs.DCSpecificRequest, reply *structs.IndexedPreparedQueries) error {
	if done, err := p.srv.forward("PreparedQuery.List", args, args, reply); done {
		return err
	}

	return p.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, queries, err := state.PreparedQueryList(ws)
			if err != nil {
				return err
			}

			reply.Index, reply.Queries = index, queries
			return p.srv.filterACL(args.Token, reply)
		})
}

func (p *PreparedQuery) Explain(args *structs.PreparedQueryExecuteRequest,
	reply *structs.PreparedQueryExplainResponse) error {
	if done, err := p.srv.forward("PreparedQuery.Explain", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"consul", "prepared-query", "explain"}, time.Now())
	defer metrics.MeasureSince([]string{"prepared-query", "explain"}, time.Now())

	p.srv.setQueryMeta(&reply.QueryMeta)
	if args.RequireConsistent {
		if err := p.srv.consistentRead(); err != nil {
			return err
		}
	}

	state := p.srv.fsm.State()
	_, query, err := state.PreparedQueryResolve(args.QueryIDOrName, args.Agent)
	if err != nil {
		return err
	}
	if query == nil {
		return ErrQueryNotFound
	}

	queries := &structs.IndexedPreparedQueries{
		Queries: structs.PreparedQueries{query},
	}
	if err := p.srv.filterACL(args.Token, queries); err != nil {
		return err
	}

	if len(queries.Queries) == 0 {
		p.srv.logger.Printf("[WARN] consul.prepared_query: Explain on prepared query '%s' denied due to ACLs", query.ID)
		return acl.ErrPermissionDenied
	}

	reply.Query = *(queries.Queries[0])
	return nil
}

func (p *PreparedQuery) Execute(args *structs.PreparedQueryExecuteRequest,
	reply *structs.PreparedQueryExecuteResponse) error {
	if done, err := p.srv.forward("PreparedQuery.Execute", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"consul", "prepared-query", "execute"}, time.Now())
	defer metrics.MeasureSince([]string{"prepared-query", "execute"}, time.Now())

	p.srv.setQueryMeta(&reply.QueryMeta)
	if args.RequireConsistent {
		if err := p.srv.consistentRead(); err != nil {
			return err
		}
	}

	state := p.srv.fsm.State()
	_, query, err := state.PreparedQueryResolve(args.QueryIDOrName, args.Agent)
	if err != nil {
		return err
	}
	if query == nil {
		return ErrQueryNotFound
	}

	if err := p.execute(query, reply); err != nil {
		return err
	}

	token := args.QueryOptions.Token
	if query.Token != "" {
		token = query.Token
	}
	if err := p.srv.filterACL(token, &reply.Nodes); err != nil {
		return err
	}

	reply.Nodes.Shuffle()

	qs := args.Source
	if qs.Datacenter == "" {
		qs.Datacenter = args.Agent.Datacenter
	}
	if query.Service.Near != "" && qs.Node == "" {
		qs.Node = query.Service.Near
	}

	if qs.Node == "_agent" {
		qs.Node = args.Agent.Node
	} else if qs.Node == "_ip" {
		if args.Source.Ip != "" {
			_, nodes, err := state.Nodes(nil)
			if err != nil {
				return err
			}

			for _, node := range nodes {
				if args.Source.Ip == node.Address {
					qs.Node = node.Node
					break
				}
			}
		} else {
			p.srv.logger.Printf("[WARN] Prepared Query using near=_ip requires " +
				"the source IP to be set but none was provided. No distance " +
				"sorting will be done.")

		}

		if qs.Node == "_ip" {
			qs.Node = ""
		}
	}

	err = p.srv.sortNodesByDistanceFrom(qs, reply.Nodes)
	if err != nil {
		return err
	}

	if qs.Node != "" && reply.Datacenter == qs.Datacenter {
		for i, node := range reply.Nodes {
			if node.Node.Node == qs.Node {
				reply.Nodes[0], reply.Nodes[i] = reply.Nodes[i], reply.Nodes[0]
				break
			}

			if i == 9 {
				break
			}
		}
	}

	if args.Limit > 0 && len(reply.Nodes) > args.Limit {
		reply.Nodes = reply.Nodes[:args.Limit]
	}

	if len(reply.Nodes) == 0 {
		wrapper := &queryServerWrapper{p.srv}
		if err := queryFailover(wrapper, query, args.Limit, args.QueryOptions, reply); err != nil {
			return err
		}
	}

	return nil
}

func (p *PreparedQuery) ExecuteRemote(args *structs.PreparedQueryExecuteRemoteRequest,
	reply *structs.PreparedQueryExecuteResponse) error {
	if done, err := p.srv.forward("PreparedQuery.ExecuteRemote", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"consul", "prepared-query", "execute_remote"}, time.Now())
	defer metrics.MeasureSince([]string{"prepared-query", "execute_remote"}, time.Now())

	p.srv.setQueryMeta(&reply.QueryMeta)
	if args.RequireConsistent {
		if err := p.srv.consistentRead(); err != nil {
			return err
		}
	}

	if err := p.execute(&args.Query, reply); err != nil {
		return err
	}

	token := args.QueryOptions.Token
	if args.Query.Token != "" {
		token = args.Query.Token
	}
	if err := p.srv.filterACL(token, &reply.Nodes); err != nil {
		return err
	}

	reply.Nodes.Shuffle()

	if args.Limit > 0 && len(reply.Nodes) > args.Limit {
		reply.Nodes = reply.Nodes[:args.Limit]
	}

	return nil
}

func (p *PreparedQuery) execute(query *structs.PreparedQuery,
	reply *structs.PreparedQueryExecuteResponse) error {
	state := p.srv.fsm.State()
	_, nodes, err := state.CheckServiceNodes(nil, query.Service.Service)
	if err != nil {
		return err
	}

	nodes = nodes.FilterIgnore(query.Service.OnlyPassing,
		query.Service.IgnoreCheckIDs)

	if len(query.Service.NodeMeta) > 0 {
		nodes = nodeMetaFilter(query.Service.NodeMeta, nodes)
	}

	if len(query.Service.Tags) > 0 {
		nodes = tagFilter(query.Service.Tags, nodes)
	}

	reply.Service = query.Service.Service
	reply.Nodes = nodes
	reply.DNS = query.DNS

	reply.Datacenter = p.srv.config.Datacenter

	return nil
}

func tagFilter(tags []string, nodes structs.CheckServiceNodes) structs.CheckServiceNodes {

	must, not := make([]string, 0), make([]string, 0)
	for _, tag := range tags {
		tag = strings.ToLower(tag)
		if strings.HasPrefix(tag, "!") {
			tag = tag[1:]
			not = append(not, tag)
		} else {
			must = append(must, tag)
		}
	}

	n := len(nodes)
	for i := 0; i < n; i++ {
		node := nodes[i]

		index := make(map[string]struct{})
		if node.Service != nil {
			for _, tag := range node.Service.Tags {
				tag = strings.ToLower(tag)
				index[tag] = struct{}{}
			}
		}

		for _, tag := range must {
			if _, ok := index[tag]; !ok {
				goto DELETE
			}
		}

		for _, tag := range not {
			if _, ok := index[tag]; ok {
				goto DELETE
			}
		}

		continue

	DELETE:
		nodes[i], nodes[n-1] = nodes[n-1], structs.CheckServiceNode{}
		n--
		i--
	}
	return nodes[:n]
}

func nodeMetaFilter(filters map[string]string, nodes structs.CheckServiceNodes) structs.CheckServiceNodes {
	var filtered structs.CheckServiceNodes
	for _, node := range nodes {
		if structs.SatisfiesMetaFilters(node.Node.Meta, filters) {
			filtered = append(filtered, node)
		}
	}
	return filtered
}

type queryServer interface {
	GetLogger() *log.Logger
	GetOtherDatacentersByDistance() ([]string, error)
	ForwardDC(method, dc string, args interface{}, reply interface{}) error
}

type queryServerWrapper struct {
	srv *Server
}

func (q *queryServerWrapper) GetLogger() *log.Logger {
	return q.srv.logger
}

func (q *queryServerWrapper) GetOtherDatacentersByDistance() ([]string, error) {

	dcs, err := q.srv.router.GetDatacentersByDistance()
	if err != nil {
		return nil, err
	}

	var result []string
	for _, dc := range dcs {
		if dc != q.srv.config.Datacenter {
			result = append(result, dc)
		}
	}
	return result, nil
}

func (q *queryServerWrapper) ForwardDC(method, dc string, args interface{}, reply interface{}) error {
	return q.srv.forwardDC(method, dc, args, reply)
}

func queryFailover(q queryServer, query *structs.PreparedQuery,
	limit int, options structs.QueryOptions,
	reply *structs.PreparedQueryExecuteResponse) error {

	nearest, err := q.GetOtherDatacentersByDistance()
	if err != nil {
		return err
	}

	known := make(map[string]struct{})
	for _, dc := range nearest {
		known[dc] = struct{}{}
	}

	var dcs []string
	index := make(map[string]struct{})
	if query.Service.Failover.NearestN > 0 {
		for i, dc := range nearest {
			if !(i < query.Service.Failover.NearestN) {
				break
			}

			dcs = append(dcs, dc)
			index[dc] = struct{}{}
		}
	}

	for _, dc := range query.Service.Failover.Datacenters {

		if _, ok := known[dc]; !ok {
			q.GetLogger().Printf("[DEBUG] consul.prepared_query: Skipping unknown datacenter '%s' in prepared query", dc)
			continue
		}

		if _, ok := index[dc]; !ok {
			dcs = append(dcs, dc)
		}
	}

	failovers := 0
	for _, dc := range dcs {

		failovers++

		reply.Nodes = nil

		remote := &structs.PreparedQueryExecuteRemoteRequest{
			Datacenter:   dc,
			Query:        *query,
			Limit:        limit,
			QueryOptions: options,
		}
		if err := q.ForwardDC("PreparedQuery.ExecuteRemote", dc, remote, reply); err != nil {
			q.GetLogger().Printf("[WARN] consul.prepared_query: Failed querying for service '%s' in datacenter '%s': %s", query.Service.Service, dc, err)
			continue
		}

		if len(reply.Nodes) > 0 {
			break
		}
	}

	reply.Failovers = failovers

	return nil
}
