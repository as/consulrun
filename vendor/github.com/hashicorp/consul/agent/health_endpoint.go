package agent

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/api"
)

func (s *HTTPServer) HealthChecksInState(resp http.ResponseWriter, req *http.Request) (interface{}, error) {

	args := structs.ChecksInStateRequest{}
	s.parseSource(req, &args.Source)
	args.NodeMetaFilters = s.parseMetaFilter(req)
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	args.State = strings.TrimPrefix(req.URL.Path, "/v1/health/state/")
	if args.State == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing check state")
		return nil, nil
	}

	var out structs.IndexedHealthChecks
	defer setMeta(resp, &out.QueryMeta)
RETRY_ONCE:
	if err := s.agent.RPC("Health.ChecksInState", &args, &out); err != nil {
		return nil, err
	}
	if args.QueryOptions.AllowStale && args.MaxStaleDuration > 0 && args.MaxStaleDuration < out.LastContact {
		args.AllowStale = false
		args.MaxStaleDuration = 0
		goto RETRY_ONCE
	}
	out.ConsistencyLevel = args.QueryOptions.ConsistencyLevel()

	if out.HealthChecks == nil {
		out.HealthChecks = make(structs.HealthChecks, 0)
	}
	for i, c := range out.HealthChecks {
		if c.ServiceTags == nil {
			clone := *c
			clone.ServiceTags = make([]string, 0)
			out.HealthChecks[i] = &clone
		}
	}
	return out.HealthChecks, nil
}

func (s *HTTPServer) HealthNodeChecks(resp http.ResponseWriter, req *http.Request) (interface{}, error) {

	args := structs.NodeSpecificRequest{}
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	args.Node = strings.TrimPrefix(req.URL.Path, "/v1/health/node/")
	if args.Node == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing node name")
		return nil, nil
	}

	var out structs.IndexedHealthChecks
	defer setMeta(resp, &out.QueryMeta)
RETRY_ONCE:
	if err := s.agent.RPC("Health.NodeChecks", &args, &out); err != nil {
		return nil, err
	}
	if args.QueryOptions.AllowStale && args.MaxStaleDuration > 0 && args.MaxStaleDuration < out.LastContact {
		args.AllowStale = false
		args.MaxStaleDuration = 0
		goto RETRY_ONCE
	}
	out.ConsistencyLevel = args.QueryOptions.ConsistencyLevel()

	if out.HealthChecks == nil {
		out.HealthChecks = make(structs.HealthChecks, 0)
	}
	for i, c := range out.HealthChecks {
		if c.ServiceTags == nil {
			clone := *c
			clone.ServiceTags = make([]string, 0)
			out.HealthChecks[i] = &clone
		}
	}
	return out.HealthChecks, nil
}

func (s *HTTPServer) HealthServiceChecks(resp http.ResponseWriter, req *http.Request) (interface{}, error) {

	args := structs.ServiceSpecificRequest{}
	s.parseSource(req, &args.Source)
	args.NodeMetaFilters = s.parseMetaFilter(req)
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	args.ServiceName = strings.TrimPrefix(req.URL.Path, "/v1/health/checks/")
	if args.ServiceName == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing service name")
		return nil, nil
	}

	var out structs.IndexedHealthChecks
	defer setMeta(resp, &out.QueryMeta)
RETRY_ONCE:
	if err := s.agent.RPC("Health.ServiceChecks", &args, &out); err != nil {
		return nil, err
	}
	if args.QueryOptions.AllowStale && args.MaxStaleDuration > 0 && args.MaxStaleDuration < out.LastContact {
		args.AllowStale = false
		args.MaxStaleDuration = 0
		goto RETRY_ONCE
	}
	out.ConsistencyLevel = args.QueryOptions.ConsistencyLevel()

	if out.HealthChecks == nil {
		out.HealthChecks = make(structs.HealthChecks, 0)
	}
	for i, c := range out.HealthChecks {
		if c.ServiceTags == nil {
			clone := *c
			clone.ServiceTags = make([]string, 0)
			out.HealthChecks[i] = &clone
		}
	}
	return out.HealthChecks, nil
}

func (s *HTTPServer) HealthServiceNodes(resp http.ResponseWriter, req *http.Request) (interface{}, error) {

	args := structs.ServiceSpecificRequest{}
	s.parseSource(req, &args.Source)
	args.NodeMetaFilters = s.parseMetaFilter(req)
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	params := req.URL.Query()
	if _, ok := params["tag"]; ok {
		args.ServiceTag = params.Get("tag")
		args.TagFilter = true
	}

	args.ServiceName = strings.TrimPrefix(req.URL.Path, "/v1/health/service/")
	if args.ServiceName == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing service name")
		return nil, nil
	}

	var out structs.IndexedCheckServiceNodes
	defer setMeta(resp, &out.QueryMeta)
RETRY_ONCE:
	if err := s.agent.RPC("Health.ServiceNodes", &args, &out); err != nil {
		return nil, err
	}
	if args.QueryOptions.AllowStale && args.MaxStaleDuration > 0 && args.MaxStaleDuration < out.LastContact {
		args.AllowStale = false
		args.MaxStaleDuration = 0
		goto RETRY_ONCE
	}
	out.ConsistencyLevel = args.QueryOptions.ConsistencyLevel()

	if _, ok := params[api.HealthPassing]; ok {
		val := params.Get(api.HealthPassing)

		var filter bool
		if val == "" {
			filter = true
		} else {
			var err error
			filter, err = strconv.ParseBool(val)
			if err != nil {
				resp.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(resp, "Invalid value for ?passing")
				return nil, nil
			}
		}

		if filter {
			out.Nodes = filterNonPassing(out.Nodes)
		}
	}

	s.agent.TranslateAddresses(args.Datacenter, out.Nodes)

	if out.Nodes == nil {
		out.Nodes = make(structs.CheckServiceNodes, 0)
	}
	for i := range out.Nodes {
		if out.Nodes[i].Checks == nil {
			out.Nodes[i].Checks = make(structs.HealthChecks, 0)
		}
		for j, c := range out.Nodes[i].Checks {
			if c.ServiceTags == nil {
				clone := *c
				clone.ServiceTags = make([]string, 0)
				out.Nodes[i].Checks[j] = &clone
			}
		}
		if out.Nodes[i].Service != nil && out.Nodes[i].Service.Tags == nil {
			clone := *out.Nodes[i].Service
			clone.Tags = make([]string, 0)
			out.Nodes[i].Service = &clone
		}
	}
	return out.Nodes, nil
}

func filterNonPassing(nodes structs.CheckServiceNodes) structs.CheckServiceNodes {
	n := len(nodes)
OUTER:
	for i := 0; i < n; i++ {
		node := nodes[i]
		for _, check := range node.Checks {
			if check.Status != api.HealthPassing {
				nodes[i], nodes[n-1] = nodes[n-1], structs.CheckServiceNode{}
				n--
				i--
				continue OUTER
			}
		}
	}
	return nodes[:n]
}
