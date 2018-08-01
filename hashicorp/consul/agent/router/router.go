package router

import (
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/as/consulrun/hashicorp/consul/agent/metadata"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/lib"
	"github.com/as/consulrun/hashicorp/consul/types"
	"github.com/as/consulrun/hashicorp/serf/coordinate"
	"github.com/as/consulrun/hashicorp/serf/serf"
)

type Router struct {
	logger *log.Logger

	localDatacenter string

	areas map[types.AreaID]*areaInfo

	managers map[string][]*Manager

	routeFn func(datacenter string) (*Manager, *metadata.Server, bool)

	isShutdown bool

	sync.RWMutex
}

type RouterSerfCluster interface {
	NumNodes() int
	Members() []serf.Member
	GetCoordinate() (*coordinate.Coordinate, error)
	GetCachedCoordinate(name string) (*coordinate.Coordinate, bool)
}

type managerInfo struct {
	manager *Manager

	shutdownCh chan struct{}
}

type areaInfo struct {
	cluster RouterSerfCluster

	pinger Pinger

	managers map[string]*managerInfo

	useTLS bool
}

func NewRouter(logger *log.Logger, localDatacenter string) *Router {
	router := &Router{
		logger:          logger,
		localDatacenter: localDatacenter,
		areas:           make(map[types.AreaID]*areaInfo),
		managers:        make(map[string][]*Manager),
	}

	router.routeFn = router.findDirectRoute

	return router
}

func (r *Router) Shutdown() {
	r.Lock()
	defer r.Unlock()

	for areaID, area := range r.areas {
		for datacenter, info := range area.managers {
			r.removeManagerFromIndex(datacenter, info.manager)
			close(info.shutdownCh)
		}

		delete(r.areas, areaID)
	}

	r.isShutdown = true
}

func (r *Router) AddArea(areaID types.AreaID, cluster RouterSerfCluster, pinger Pinger, useTLS bool) error {
	r.Lock()
	defer r.Unlock()

	if r.isShutdown {
		return fmt.Errorf("cannot add area, router is shut down")
	}

	if _, ok := r.areas[areaID]; ok {
		return fmt.Errorf("area ID %q already exists", areaID)
	}

	area := &areaInfo{
		cluster:  cluster,
		pinger:   pinger,
		managers: make(map[string]*managerInfo),
		useTLS:   useTLS,
	}
	r.areas[areaID] = area

	for _, m := range cluster.Members() {
		ok, parts := metadata.IsConsulServer(m)
		if !ok {
			r.logger.Printf("[WARN]: consul: Non-server %q in server-only area %q",
				m.Name, areaID)
			continue
		}

		if err := r.addServer(area, parts); err != nil {
			return fmt.Errorf("failed to add server %q to area %q: %v", m.Name, areaID, err)
		}
	}

	return nil
}

func (r *Router) removeManagerFromIndex(datacenter string, manager *Manager) {
	managers := r.managers[datacenter]
	for i := 0; i < len(managers); i++ {
		if managers[i] == manager {
			r.managers[datacenter] = append(managers[:i], managers[i+1:]...)
			if len(r.managers[datacenter]) == 0 {
				delete(r.managers, datacenter)
			}
			return
		}
	}
	panic("managers index out of sync")
}

func (r *Router) TLSEnabled(areaID types.AreaID) (bool, error) {
	r.RLock()
	defer r.RUnlock()

	area, ok := r.areas[areaID]
	if !ok {
		return false, fmt.Errorf("area ID %q does not exist", areaID)
	}

	return area.useTLS, nil
}

func (r *Router) RemoveArea(areaID types.AreaID) error {
	r.Lock()
	defer r.Unlock()

	area, ok := r.areas[areaID]
	if !ok {
		return fmt.Errorf("area ID %q does not exist", areaID)
	}

	for datacenter, info := range area.managers {
		r.removeManagerFromIndex(datacenter, info.manager)
		close(info.shutdownCh)
	}

	delete(r.areas, areaID)
	return nil
}

func (r *Router) addServer(area *areaInfo, s *metadata.Server) error {

	info, ok := area.managers[s.Datacenter]
	if !ok {
		shutdownCh := make(chan struct{})
		manager := New(r.logger, shutdownCh, area.cluster, area.pinger)
		info = &managerInfo{
			manager:    manager,
			shutdownCh: shutdownCh,
		}
		area.managers[s.Datacenter] = info

		managers := r.managers[s.Datacenter]
		r.managers[s.Datacenter] = append(managers, manager)
		go manager.Start()
	}

	if area.useTLS {
		s.UseTLS = true
	}

	info.manager.AddServer(s)
	return nil
}

func (r *Router) AddServer(areaID types.AreaID, s *metadata.Server) error {
	r.Lock()
	defer r.Unlock()

	area, ok := r.areas[areaID]
	if !ok {
		return fmt.Errorf("area ID %q does not exist", areaID)
	}
	return r.addServer(area, s)
}

func (r *Router) RemoveServer(areaID types.AreaID, s *metadata.Server) error {
	r.Lock()
	defer r.Unlock()

	area, ok := r.areas[areaID]
	if !ok {
		return fmt.Errorf("area ID %q does not exist", areaID)
	}

	info, ok := area.managers[s.Datacenter]
	if !ok {
		return nil
	}
	info.manager.RemoveServer(s)

	if num := info.manager.NumServers(); num == 0 {
		r.removeManagerFromIndex(s.Datacenter, info.manager)
		close(info.shutdownCh)
		delete(area.managers, s.Datacenter)
	}

	return nil
}

func (r *Router) FailServer(areaID types.AreaID, s *metadata.Server) error {
	r.RLock()
	defer r.RUnlock()

	area, ok := r.areas[areaID]
	if !ok {
		return fmt.Errorf("area ID %q does not exist", areaID)
	}

	info, ok := area.managers[s.Datacenter]
	if !ok {
		return nil
	}

	info.manager.NotifyFailedServer(s)
	return nil
}

func (r *Router) FindRoute(datacenter string) (*Manager, *metadata.Server, bool) {
	return r.routeFn(datacenter)
}

func (r *Router) findDirectRoute(datacenter string) (*Manager, *metadata.Server, bool) {
	r.RLock()
	defer r.RUnlock()

	managers, ok := r.managers[datacenter]
	if !ok {
		return nil, nil, false
	}

	for _, manager := range managers {
		if manager.IsOffline() {
			continue
		}

		if s := manager.FindServer(); s != nil {
			return manager, s, true
		}
	}

	return nil, nil, false
}

func (r *Router) GetDatacenters() []string {
	r.RLock()
	defer r.RUnlock()

	dcs := make([]string, 0, len(r.managers))
	for dc := range r.managers {
		dcs = append(dcs, dc)
	}

	sort.Strings(dcs)
	return dcs
}

type datacenterSorter struct {
	Names []string
	Vec   []float64
}

func (n *datacenterSorter) Len() int {
	return len(n.Names)
}

func (n *datacenterSorter) Swap(i, j int) {
	n.Names[i], n.Names[j] = n.Names[j], n.Names[i]
	n.Vec[i], n.Vec[j] = n.Vec[j], n.Vec[i]
}

func (n *datacenterSorter) Less(i, j int) bool {
	return n.Vec[i] < n.Vec[j]
}

func (r *Router) GetDatacentersByDistance() ([]string, error) {
	r.RLock()
	defer r.RUnlock()

	dcs := make(map[string]float64)
	for areaID, info := range r.areas {
		index := make(map[string][]float64)
		coord, err := info.cluster.GetCoordinate()
		if err != nil {
			return nil, err
		}

		for _, m := range info.cluster.Members() {
			ok, parts := metadata.IsConsulServer(m)
			if !ok {
				r.logger.Printf("[WARN]: consul: Non-server %q in server-only area %q",
					m.Name, areaID)
				continue
			}

			existing := index[parts.Datacenter]
			if parts.Datacenter == r.localDatacenter {

				index[parts.Datacenter] = append(existing, 0.0)
			} else {

				other, _ := info.cluster.GetCachedCoordinate(parts.Name)
				rtt := lib.ComputeDistance(coord, other)
				index[parts.Datacenter] = append(existing, rtt)
			}
		}

		for dc, rtts := range index {
			sort.Float64s(rtts)
			rtt := rtts[len(rtts)/2]

			current, ok := dcs[dc]
			if !ok || (ok && rtt < current) {
				dcs[dc] = rtt
			}
		}
	}

	names := make([]string, 0, len(dcs))
	for dc := range dcs {
		names = append(names, dc)
	}
	sort.Strings(names)

	rtts := make([]float64, 0, len(dcs))
	for _, dc := range names {
		rtts = append(rtts, dcs[dc])
	}
	sort.Stable(&datacenterSorter{names, rtts})
	return names, nil
}

func (r *Router) GetDatacenterMaps() ([]structs.DatacenterMap, error) {
	r.RLock()
	defer r.RUnlock()

	var maps []structs.DatacenterMap
	for areaID, info := range r.areas {
		index := make(map[string]structs.Coordinates)
		for _, m := range info.cluster.Members() {
			ok, parts := metadata.IsConsulServer(m)
			if !ok {
				r.logger.Printf("[WARN]: consul: Non-server %q in server-only area %q",
					m.Name, areaID)
				continue
			}

			coord, ok := info.cluster.GetCachedCoordinate(parts.Name)
			if ok {
				entry := &structs.Coordinate{
					Node:  parts.Name,
					Coord: coord,
				}
				existing := index[parts.Datacenter]
				index[parts.Datacenter] = append(existing, entry)
			}
		}

		for dc, coords := range index {
			entry := structs.DatacenterMap{
				Datacenter:  dc,
				AreaID:      areaID,
				Coordinates: coords,
			}
			maps = append(maps, entry)
		}
	}
	return maps, nil
}
