package consul

import (
	"fmt"
	"time"

	"github.com/armon/go-metrics"
	"github.com/as/consulrun/hashicorp/consul/acl"
	"github.com/as/consulrun/hashicorp/consul/agent/consul/state"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/ipaddr"
	"github.com/as/consulrun/hashicorp/consul/types"
	"github.com/as/consulrun/hashicorp/go-memdb"
	"github.com/as/consulrun/hashicorp/go-uuid"
)

type Catalog struct {
	srv *Server
}

func (c *Catalog) Register(args *structs.RegisterRequest, reply *struct{}) error {
	if done, err := c.srv.forward("Catalog.Register", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"consul", "catalog", "register"}, time.Now())
	defer metrics.MeasureSince([]string{"catalog", "register"}, time.Now())

	if args.Node == "" {
		return fmt.Errorf("Must provide node")
	}
	if args.Address == "" && !args.SkipNodeUpdate {
		return fmt.Errorf("Must provide address if SkipNodeUpdate is not set")
	}
	if args.ID != "" {
		if _, err := uuid.ParseUUID(string(args.ID)); err != nil {
			return fmt.Errorf("Bad node ID: %v", err)
		}
	}

	rule, err := c.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}

	if args.Service != nil {

		if args.Service.ID == "" && args.Service.Service != "" {
			args.Service.ID = args.Service.Service
		}

		if args.Service.ID != "" && args.Service.Service == "" {
			return fmt.Errorf("Must provide service name with ID")
		}

		if ipaddr.IsAny(args.Service.Address) {
			return fmt.Errorf("Invalid service address")
		}

		if args.Service.Service != structs.ConsulServiceName {
			if rule != nil && !rule.ServiceWrite(args.Service.Service, nil) {
				return acl.ErrPermissionDenied
			}
		}
	}

	if args.Check != nil {
		args.Checks = append(args.Checks, args.Check)
		args.Check = nil
	}
	for _, check := range args.Checks {
		if check.CheckID == "" && check.Name != "" {
			check.CheckID = types.CheckID(check.Name)
		}
		if check.Node == "" {
			check.Node = args.Node
		}
	}

	if rule != nil && c.srv.config.ACLEnforceVersion8 {
		state := c.srv.fsm.State()
		_, ns, err := state.NodeServices(nil, args.Node)
		if err != nil {
			return fmt.Errorf("Node lookup failed: %v", err)
		}
		if err := vetRegisterWithACL(rule, args, ns); err != nil {
			return err
		}
	}

	resp, err := c.srv.raftApply(structs.RegisterRequestType, args)
	if err != nil {
		return err
	}
	if respErr, ok := resp.(error); ok {
		return respErr
	}
	return nil
}

func (c *Catalog) Deregister(args *structs.DeregisterRequest, reply *struct{}) error {
	if done, err := c.srv.forward("Catalog.Deregister", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"consul", "catalog", "deregister"}, time.Now())
	defer metrics.MeasureSince([]string{"catalog", "deregister"}, time.Now())

	if args.Node == "" {
		return fmt.Errorf("Must provide node")
	}

	rule, err := c.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}

	if rule != nil && c.srv.config.ACLEnforceVersion8 {
		state := c.srv.fsm.State()

		var ns *structs.NodeService
		if args.ServiceID != "" {
			_, ns, err = state.NodeService(args.Node, args.ServiceID)
			if err != nil {
				return fmt.Errorf("Service lookup failed: %v", err)
			}
		}

		var nc *structs.HealthCheck
		if args.CheckID != "" {
			_, nc, err = state.NodeCheck(args.Node, args.CheckID)
			if err != nil {
				return fmt.Errorf("Check lookup failed: %v", err)
			}
		}

		if err := vetDeregisterWithACL(rule, args, ns, nc); err != nil {
			return err
		}

	}

	if _, err := c.srv.raftApply(structs.DeregisterRequestType, args); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) ListDatacenters(args *struct{}, reply *[]string) error {
	dcs, err := c.srv.router.GetDatacentersByDistance()
	if err != nil {
		return err
	}

	if len(dcs) == 0 { // no WAN federation, so return the local data center name
		dcs = []string{c.srv.config.Datacenter}
	}

	*reply = dcs
	return nil
}

func (c *Catalog) ListNodes(args *structs.DCSpecificRequest, reply *structs.IndexedNodes) error {
	if done, err := c.srv.forward("Catalog.ListNodes", args, args, reply); done {
		return err
	}

	return c.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			var index uint64
			var nodes structs.Nodes
			var err error
			if len(args.NodeMetaFilters) > 0 {
				index, nodes, err = state.NodesByMeta(ws, args.NodeMetaFilters)
			} else {
				index, nodes, err = state.Nodes(ws)
			}
			if err != nil {
				return err
			}

			reply.Index, reply.Nodes = index, nodes
			if err := c.srv.filterACL(args.Token, reply); err != nil {
				return err
			}
			return c.srv.sortNodesByDistanceFrom(args.Source, reply.Nodes)
		})
}

func (c *Catalog) ListServices(args *structs.DCSpecificRequest, reply *structs.IndexedServices) error {
	if done, err := c.srv.forward("Catalog.ListServices", args, args, reply); done {
		return err
	}

	return c.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			var index uint64
			var services structs.Services
			var err error
			if len(args.NodeMetaFilters) > 0 {
				index, services, err = state.ServicesByNodeMeta(ws, args.NodeMetaFilters)
			} else {
				index, services, err = state.Services(ws)
			}
			if err != nil {
				return err
			}

			reply.Index, reply.Services = index, services
			return c.srv.filterACL(args.Token, reply)
		})
}

func (c *Catalog) ServiceNodes(args *structs.ServiceSpecificRequest, reply *structs.IndexedServiceNodes) error {
	if done, err := c.srv.forward("Catalog.ServiceNodes", args, args, reply); done {
		return err
	}

	if args.ServiceName == "" {
		return fmt.Errorf("Must provide service name")
	}

	err := c.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			var index uint64
			var services structs.ServiceNodes
			var err error
			if args.TagFilter {
				index, services, err = state.ServiceTagNodes(ws, args.ServiceName, args.ServiceTag)
			} else {
				index, services, err = state.ServiceNodes(ws, args.ServiceName)
			}
			if err != nil {
				return err
			}
			reply.Index, reply.ServiceNodes = index, services
			if len(args.NodeMetaFilters) > 0 {
				var filtered structs.ServiceNodes
				for _, service := range services {
					if structs.SatisfiesMetaFilters(service.NodeMeta, args.NodeMetaFilters) {
						filtered = append(filtered, service)
					}
				}
				reply.ServiceNodes = filtered
			}
			if err := c.srv.filterACL(args.Token, reply); err != nil {
				return err
			}
			return c.srv.sortNodesByDistanceFrom(args.Source, reply.ServiceNodes)
		})

	if err == nil {
		metrics.IncrCounterWithLabels([]string{"consul", "catalog", "service", "query"}, 1,
			[]metrics.Label{{Name: "service", Value: args.ServiceName}})
		metrics.IncrCounterWithLabels([]string{"catalog", "service", "query"}, 1,
			[]metrics.Label{{Name: "service", Value: args.ServiceName}})
		if args.ServiceTag != "" {
			metrics.IncrCounterWithLabels([]string{"consul", "catalog", "service", "query-tag"}, 1,
				[]metrics.Label{{Name: "service", Value: args.ServiceName}, {Name: "tag", Value: args.ServiceTag}})
			metrics.IncrCounterWithLabels([]string{"catalog", "service", "query-tag"}, 1,
				[]metrics.Label{{Name: "service", Value: args.ServiceName}, {Name: "tag", Value: args.ServiceTag}})
		}
		if len(reply.ServiceNodes) == 0 {
			metrics.IncrCounterWithLabels([]string{"consul", "catalog", "service", "not-found"}, 1,
				[]metrics.Label{{Name: "service", Value: args.ServiceName}})
			metrics.IncrCounterWithLabels([]string{"catalog", "service", "not-found"}, 1,
				[]metrics.Label{{Name: "service", Value: args.ServiceName}})
		}
	}
	return err
}

func (c *Catalog) NodeServices(args *structs.NodeSpecificRequest, reply *structs.IndexedNodeServices) error {
	if done, err := c.srv.forward("Catalog.NodeServices", args, args, reply); done {
		return err
	}

	if args.Node == "" {
		return fmt.Errorf("Must provide node")
	}

	return c.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, services, err := state.NodeServices(ws, args.Node)
			if err != nil {
				return err
			}

			reply.Index, reply.NodeServices = index, services
			return c.srv.filterACL(args.Token, reply)
		})
}
