package agent

import (
	"fmt"
	"net/http"
	"strings"

	metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/consul/agent/structs"
)

var durations = NewDurationFixer("interval", "timeout", "deregistercriticalserviceafter")

func (s *HTTPServer) CatalogRegister(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	metrics.IncrCounterWithLabels([]string{"client", "api", "catalog_register"}, 1,
		[]metrics.Label{{Name: "node", Value: s.nodeName()}})

	var args structs.RegisterRequest
	if err := decodeBody(req, &args, durations.FixupDurations); err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(resp, "Request decode failed: %v", err)
		return nil, nil
	}

	if args.Datacenter == "" {
		args.Datacenter = s.agent.config.Datacenter
	}
	s.parseToken(req, &args.Token)

	var out struct{}
	if err := s.agent.RPC("Catalog.Register", &args, &out); err != nil {
		metrics.IncrCounterWithLabels([]string{"client", "rpc", "error", "catalog_register"}, 1,
			[]metrics.Label{{Name: "node", Value: s.nodeName()}})
		return nil, err
	}
	metrics.IncrCounterWithLabels([]string{"client", "api", "success", "catalog_register"}, 1,
		[]metrics.Label{{Name: "node", Value: s.nodeName()}})
	return true, nil
}

func (s *HTTPServer) CatalogDeregister(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	metrics.IncrCounterWithLabels([]string{"client", "api", "catalog_deregister"}, 1,
		[]metrics.Label{{Name: "node", Value: s.nodeName()}})

	var args structs.DeregisterRequest
	if err := decodeBody(req, &args, nil); err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(resp, "Request decode failed: %v", err)
		return nil, nil
	}

	if args.Datacenter == "" {
		args.Datacenter = s.agent.config.Datacenter
	}
	s.parseToken(req, &args.Token)

	var out struct{}
	if err := s.agent.RPC("Catalog.Deregister", &args, &out); err != nil {
		metrics.IncrCounterWithLabels([]string{"client", "rpc", "error", "catalog_deregister"}, 1,
			[]metrics.Label{{Name: "node", Value: s.nodeName()}})
		return nil, err
	}
	metrics.IncrCounterWithLabels([]string{"client", "api", "success", "catalog_deregister"}, 1,
		[]metrics.Label{{Name: "node", Value: s.nodeName()}})
	return true, nil
}

func (s *HTTPServer) CatalogDatacenters(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	metrics.IncrCounterWithLabels([]string{"client", "api", "catalog_datacenters"}, 1,
		[]metrics.Label{{Name: "node", Value: s.nodeName()}})

	var out []string
	if err := s.agent.RPC("Catalog.ListDatacenters", struct{}{}, &out); err != nil {
		metrics.IncrCounterWithLabels([]string{"client", "rpc", "error", "catalog_datacenters"}, 1,
			[]metrics.Label{{Name: "node", Value: s.nodeName()}})
		return nil, err
	}
	metrics.IncrCounterWithLabels([]string{"client", "api", "success", "catalog_datacenters"}, 1,
		[]metrics.Label{{Name: "node", Value: s.nodeName()}})
	return out, nil
}

func (s *HTTPServer) CatalogNodes(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	metrics.IncrCounterWithLabels([]string{"client", "api", "catalog_nodes"}, 1,
		[]metrics.Label{{Name: "node", Value: s.nodeName()}})

	args := structs.DCSpecificRequest{}
	s.parseSource(req, &args.Source)
	args.NodeMetaFilters = s.parseMetaFilter(req)
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		metrics.IncrCounterWithLabels([]string{"client", "rpc", "error", "catalog_nodes"}, 1,
			[]metrics.Label{{Name: "node", Value: s.nodeName()}})
		return nil, nil
	}

	var out structs.IndexedNodes
	defer setMeta(resp, &out.QueryMeta)
RETRY_ONCE:
	if err := s.agent.RPC("Catalog.ListNodes", &args, &out); err != nil {
		return nil, err
	}
	if args.QueryOptions.AllowStale && args.MaxStaleDuration > 0 && args.MaxStaleDuration < out.LastContact {
		args.AllowStale = false
		args.MaxStaleDuration = 0
		goto RETRY_ONCE
	}
	out.ConsistencyLevel = args.QueryOptions.ConsistencyLevel()

	s.agent.TranslateAddresses(args.Datacenter, out.Nodes)

	if out.Nodes == nil {
		out.Nodes = make(structs.Nodes, 0)
	}
	metrics.IncrCounterWithLabels([]string{"client", "api", "success", "catalog_nodes"}, 1,
		[]metrics.Label{{Name: "node", Value: s.nodeName()}})
	return out.Nodes, nil
}

func (s *HTTPServer) CatalogServices(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	metrics.IncrCounterWithLabels([]string{"client", "api", "catalog_services"}, 1,
		[]metrics.Label{{Name: "node", Value: s.nodeName()}})

	args := structs.DCSpecificRequest{}
	args.NodeMetaFilters = s.parseMetaFilter(req)
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	var out structs.IndexedServices
	defer setMeta(resp, &out.QueryMeta)
RETRY_ONCE:
	if err := s.agent.RPC("Catalog.ListServices", &args, &out); err != nil {
		metrics.IncrCounterWithLabels([]string{"client", "rpc", "error", "catalog_services"}, 1,
			[]metrics.Label{{Name: "node", Value: s.nodeName()}})
		return nil, err
	}
	if args.QueryOptions.AllowStale && args.MaxStaleDuration > 0 && args.MaxStaleDuration < out.LastContact {
		args.AllowStale = false
		args.MaxStaleDuration = 0
		goto RETRY_ONCE
	}
	out.ConsistencyLevel = args.QueryOptions.ConsistencyLevel()

	if out.Services == nil {
		out.Services = make(structs.Services, 0)
	}
	metrics.IncrCounterWithLabels([]string{"client", "api", "success", "catalog_services"}, 1,
		[]metrics.Label{{Name: "node", Value: s.nodeName()}})
	return out.Services, nil
}

func (s *HTTPServer) CatalogServiceNodes(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	metrics.IncrCounterWithLabels([]string{"client", "api", "catalog_service_nodes"}, 1,
		[]metrics.Label{{Name: "node", Value: s.nodeName()}})

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

	args.ServiceName = strings.TrimPrefix(req.URL.Path, "/v1/catalog/service/")
	if args.ServiceName == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing service name")
		return nil, nil
	}

	var out structs.IndexedServiceNodes
	defer setMeta(resp, &out.QueryMeta)
RETRY_ONCE:
	if err := s.agent.RPC("Catalog.ServiceNodes", &args, &out); err != nil {
		metrics.IncrCounterWithLabels([]string{"client", "rpc", "error", "catalog_service_nodes"}, 1,
			[]metrics.Label{{Name: "node", Value: s.nodeName()}})
		return nil, err
	}
	if args.QueryOptions.AllowStale && args.MaxStaleDuration > 0 && args.MaxStaleDuration < out.LastContact {
		args.AllowStale = false
		args.MaxStaleDuration = 0
		goto RETRY_ONCE
	}
	out.ConsistencyLevel = args.QueryOptions.ConsistencyLevel()
	s.agent.TranslateAddresses(args.Datacenter, out.ServiceNodes)

	if out.ServiceNodes == nil {
		out.ServiceNodes = make(structs.ServiceNodes, 0)
	}
	for i, s := range out.ServiceNodes {
		if s.ServiceTags == nil {
			clone := *s
			clone.ServiceTags = make([]string, 0)
			out.ServiceNodes[i] = &clone
		}
	}
	metrics.IncrCounterWithLabels([]string{"client", "api", "success", "catalog_service_nodes"}, 1,
		[]metrics.Label{{Name: "node", Value: s.nodeName()}})
	return out.ServiceNodes, nil
}

func (s *HTTPServer) CatalogNodeServices(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	metrics.IncrCounterWithLabels([]string{"client", "api", "catalog_node_services"}, 1,
		[]metrics.Label{{Name: "node", Value: s.nodeName()}})

	args := structs.NodeSpecificRequest{}
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	args.Node = strings.TrimPrefix(req.URL.Path, "/v1/catalog/node/")
	if args.Node == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing node name")
		return nil, nil
	}

	var out structs.IndexedNodeServices
	defer setMeta(resp, &out.QueryMeta)
RETRY_ONCE:
	if err := s.agent.RPC("Catalog.NodeServices", &args, &out); err != nil {
		metrics.IncrCounterWithLabels([]string{"client", "rpc", "error", "catalog_node_services"}, 1,
			[]metrics.Label{{Name: "node", Value: s.nodeName()}})
		return nil, err
	}
	if args.QueryOptions.AllowStale && args.MaxStaleDuration > 0 && args.MaxStaleDuration < out.LastContact {
		args.AllowStale = false
		args.MaxStaleDuration = 0
		goto RETRY_ONCE
	}
	out.ConsistencyLevel = args.QueryOptions.ConsistencyLevel()
	if out.NodeServices != nil && out.NodeServices.Node != nil {
		s.agent.TranslateAddresses(args.Datacenter, out.NodeServices.Node)
	}

	if out.NodeServices != nil {
		for _, s := range out.NodeServices.Services {
			if s.Tags == nil {
				s.Tags = make([]string, 0)
			}
		}
	}
	metrics.IncrCounterWithLabels([]string{"client", "api", "success", "catalog_node_services"}, 1,
		[]metrics.Label{{Name: "node", Value: s.nodeName()}})
	return out.NodeServices, nil
}