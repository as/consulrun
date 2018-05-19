package agent

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/api"
)

type ServiceSummary struct {
	Name           string
	Tags           []string
	Nodes          []string
	ChecksPassing  int
	ChecksWarning  int
	ChecksCritical int
}

func (s *HTTPServer) UINodes(resp http.ResponseWriter, req *http.Request) (interface{}, error) {

	args := structs.DCSpecificRequest{}
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	var out structs.IndexedNodeDump
	defer setMeta(resp, &out.QueryMeta)
RPC:
	if err := s.agent.RPC("Internal.NodeDump", &args, &out); err != nil {

		if strings.Contains(err.Error(), structs.ErrNoLeader.Error()) && !args.AllowStale {
			args.AllowStale = true
			goto RPC
		}
		return nil, err
	}

	for _, info := range out.Dump {
		if info.Services == nil {
			info.Services = make([]*structs.NodeService, 0)
		}
		if info.Checks == nil {
			info.Checks = make([]*structs.HealthCheck, 0)
		}
	}
	if out.Dump == nil {
		out.Dump = make(structs.NodeDump, 0)
	}
	return out.Dump, nil
}

func (s *HTTPServer) UINodeInfo(resp http.ResponseWriter, req *http.Request) (interface{}, error) {

	args := structs.NodeSpecificRequest{}
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	args.Node = strings.TrimPrefix(req.URL.Path, "/v1/internal/ui/node/")
	if args.Node == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing node name")
		return nil, nil
	}

	var out structs.IndexedNodeDump
	defer setMeta(resp, &out.QueryMeta)
RPC:
	if err := s.agent.RPC("Internal.NodeInfo", &args, &out); err != nil {

		if strings.Contains(err.Error(), structs.ErrNoLeader.Error()) && !args.AllowStale {
			args.AllowStale = true
			goto RPC
		}
		return nil, err
	}

	if len(out.Dump) > 0 {
		info := out.Dump[0]
		if info.Services == nil {
			info.Services = make([]*structs.NodeService, 0)
		}
		if info.Checks == nil {
			info.Checks = make([]*structs.HealthCheck, 0)
		}
		return info, nil
	}

	resp.WriteHeader(http.StatusNotFound)
	return nil, nil
}

func (s *HTTPServer) UIServices(resp http.ResponseWriter, req *http.Request) (interface{}, error) {

	args := structs.DCSpecificRequest{}
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	var out structs.IndexedNodeDump
	defer setMeta(resp, &out.QueryMeta)
RPC:
	if err := s.agent.RPC("Internal.NodeDump", &args, &out); err != nil {

		if strings.Contains(err.Error(), structs.ErrNoLeader.Error()) && !args.AllowStale {
			args.AllowStale = true
			goto RPC
		}
		return nil, err
	}

	return summarizeServices(out.Dump), nil
}

func summarizeServices(dump structs.NodeDump) []*ServiceSummary {

	var services []string
	summary := make(map[string]*ServiceSummary)
	getService := func(service string) *ServiceSummary {
		serv, ok := summary[service]
		if !ok {
			serv = &ServiceSummary{Name: service}
			summary[service] = serv
			services = append(services, service)
		}
		return serv
	}

	for _, node := range dump {
		nodeServices := make([]*ServiceSummary, len(node.Services))
		for idx, service := range node.Services {
			sum := getService(service.Service)
			sum.Tags = service.Tags
			sum.Nodes = append(sum.Nodes, node.Node)
			nodeServices[idx] = sum
		}
		for _, check := range node.Checks {
			var services []*ServiceSummary
			if check.ServiceName == "" {
				services = nodeServices
			} else {
				services = []*ServiceSummary{getService(check.ServiceName)}
			}
			for _, sum := range services {
				switch check.Status {
				case api.HealthPassing:
					sum.ChecksPassing++
				case api.HealthWarning:
					sum.ChecksWarning++
				case api.HealthCritical:
					sum.ChecksCritical++
				}
			}
		}
	}

	sort.Strings(services)
	output := make([]*ServiceSummary, len(summary))
	for idx, service := range services {

		sum := summary[service]
		sort.Strings(sum.Nodes)
		output[idx] = sum
	}
	return output
}
