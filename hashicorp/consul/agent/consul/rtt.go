package consul

import (
	"fmt"
	"sort"

	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/lib"
)

type nodeSorter struct {
	Nodes structs.Nodes
	Vec   []float64
}

func (s *Server) newNodeSorter(cs lib.CoordinateSet, nodes structs.Nodes) (sort.Interface, error) {
	state := s.fsm.State()
	vec := make([]float64, len(nodes))
	for i, node := range nodes {
		_, other, err := state.Coordinate(node.Node, nil)
		if err != nil {
			return nil, err
		}
		c1, c2 := cs.Intersect(other)
		vec[i] = lib.ComputeDistance(c1, c2)
	}
	return &nodeSorter{nodes, vec}, nil
}

func (n *nodeSorter) Len() int {
	return len(n.Nodes)
}

func (n *nodeSorter) Swap(i, j int) {
	n.Nodes[i], n.Nodes[j] = n.Nodes[j], n.Nodes[i]
	n.Vec[i], n.Vec[j] = n.Vec[j], n.Vec[i]
}

func (n *nodeSorter) Less(i, j int) bool {
	return n.Vec[i] < n.Vec[j]
}

type serviceNodeSorter struct {
	Nodes structs.ServiceNodes
	Vec   []float64
}

func (s *Server) newServiceNodeSorter(cs lib.CoordinateSet, nodes structs.ServiceNodes) (sort.Interface, error) {
	state := s.fsm.State()
	vec := make([]float64, len(nodes))
	for i, node := range nodes {
		_, other, err := state.Coordinate(node.Node, nil)
		if err != nil {
			return nil, err
		}
		c1, c2 := cs.Intersect(other)
		vec[i] = lib.ComputeDistance(c1, c2)
	}
	return &serviceNodeSorter{nodes, vec}, nil
}

func (n *serviceNodeSorter) Len() int {
	return len(n.Nodes)
}

func (n *serviceNodeSorter) Swap(i, j int) {
	n.Nodes[i], n.Nodes[j] = n.Nodes[j], n.Nodes[i]
	n.Vec[i], n.Vec[j] = n.Vec[j], n.Vec[i]
}

func (n *serviceNodeSorter) Less(i, j int) bool {
	return n.Vec[i] < n.Vec[j]
}

type healthCheckSorter struct {
	Checks structs.HealthChecks
	Vec    []float64
}

func (s *Server) newHealthCheckSorter(cs lib.CoordinateSet, checks structs.HealthChecks) (sort.Interface, error) {
	state := s.fsm.State()
	vec := make([]float64, len(checks))
	for i, check := range checks {
		_, other, err := state.Coordinate(check.Node, nil)
		if err != nil {
			return nil, err
		}
		c1, c2 := cs.Intersect(other)
		vec[i] = lib.ComputeDistance(c1, c2)
	}
	return &healthCheckSorter{checks, vec}, nil
}

func (n *healthCheckSorter) Len() int {
	return len(n.Checks)
}

func (n *healthCheckSorter) Swap(i, j int) {
	n.Checks[i], n.Checks[j] = n.Checks[j], n.Checks[i]
	n.Vec[i], n.Vec[j] = n.Vec[j], n.Vec[i]
}

func (n *healthCheckSorter) Less(i, j int) bool {
	return n.Vec[i] < n.Vec[j]
}

type checkServiceNodeSorter struct {
	Nodes structs.CheckServiceNodes
	Vec   []float64
}

func (s *Server) newCheckServiceNodeSorter(cs lib.CoordinateSet, nodes structs.CheckServiceNodes) (sort.Interface, error) {
	state := s.fsm.State()
	vec := make([]float64, len(nodes))
	for i, node := range nodes {
		_, other, err := state.Coordinate(node.Node.Node, nil)
		if err != nil {
			return nil, err
		}
		c1, c2 := cs.Intersect(other)
		vec[i] = lib.ComputeDistance(c1, c2)
	}
	return &checkServiceNodeSorter{nodes, vec}, nil
}

func (n *checkServiceNodeSorter) Len() int {
	return len(n.Nodes)
}

func (n *checkServiceNodeSorter) Swap(i, j int) {
	n.Nodes[i], n.Nodes[j] = n.Nodes[j], n.Nodes[i]
	n.Vec[i], n.Vec[j] = n.Vec[j], n.Vec[i]
}

func (n *checkServiceNodeSorter) Less(i, j int) bool {
	return n.Vec[i] < n.Vec[j]
}

func (s *Server) newSorterByDistanceFrom(cs lib.CoordinateSet, subj interface{}) (sort.Interface, error) {
	switch v := subj.(type) {
	case structs.Nodes:
		return s.newNodeSorter(cs, v)
	case structs.ServiceNodes:
		return s.newServiceNodeSorter(cs, v)
	case structs.HealthChecks:
		return s.newHealthCheckSorter(cs, v)
	case structs.CheckServiceNodes:
		return s.newCheckServiceNodeSorter(cs, v)
	default:
		panic(fmt.Errorf("Unhandled type passed to newSorterByDistanceFrom: %#v", subj))
	}
}

//
func (s *Server) sortNodesByDistanceFrom(source structs.QuerySource, subj interface{}) error {

	if source.Node == "" {
		return nil
	}

	if source.Datacenter != s.config.Datacenter {
		return nil
	}

	state := s.fsm.State()
	_, cs, err := state.Coordinate(source.Node, nil)
	if err != nil {
		return err
	}
	if len(cs) == 0 {
		return nil
	}

	sorter, err := s.newSorterByDistanceFrom(cs, subj)
	if err != nil {
		return err
	}
	sort.Stable(sorter)
	return nil
}
