package consul

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/as/consulrun/hashicorp/consul/acl"
	"github.com/as/consulrun/hashicorp/consul/agent/consul/state"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/go-memdb"
)

type Coordinate struct {
	srv *Server

	updates map[string]*structs.CoordinateUpdateRequest

	updatesLock sync.Mutex
}

func NewCoordinate(srv *Server) *Coordinate {
	c := &Coordinate{
		srv:     srv,
		updates: make(map[string]*structs.CoordinateUpdateRequest),
	}

	go c.batchUpdate()
	return c
}

func (c *Coordinate) batchUpdate() {
	for {
		select {
		case <-time.After(c.srv.config.CoordinateUpdatePeriod):
			if err := c.batchApplyUpdates(); err != nil {
				c.srv.logger.Printf("[WARN] consul.coordinate: Batch update failed: %v", err)
			}
		case <-c.srv.shutdownCh:
			return
		}
	}
}

func (c *Coordinate) batchApplyUpdates() error {

	c.updatesLock.Lock()
	pending := c.updates
	c.updates = make(map[string]*structs.CoordinateUpdateRequest)
	c.updatesLock.Unlock()

	limit := c.srv.config.CoordinateUpdateBatchSize * c.srv.config.CoordinateUpdateMaxBatches
	size := len(pending)
	if size > limit {
		c.srv.logger.Printf("[WARN] consul.coordinate: Discarded %d coordinate updates", size-limit)
		size = limit
	}

	i := 0
	updates := make(structs.Coordinates, size)
	for _, update := range pending {
		if !(i < size) {
			break
		}

		updates[i] = &structs.Coordinate{
			Node:    update.Node,
			Segment: update.Segment,
			Coord:   update.Coord,
		}
		i++
	}

	for start := 0; start < size; start += c.srv.config.CoordinateUpdateBatchSize {
		end := start + c.srv.config.CoordinateUpdateBatchSize
		if end > size {
			end = size
		}

		t := structs.CoordinateBatchUpdateType | structs.IgnoreUnknownTypeFlag

		slice := updates[start:end]
		resp, err := c.srv.raftApply(t, slice)
		if err != nil {
			return err
		}
		if respErr, ok := resp.(error); ok {
			return respErr
		}
	}
	return nil
}

func (c *Coordinate) Update(args *structs.CoordinateUpdateRequest, reply *struct{}) (err error) {
	if done, err := c.srv.forward("Coordinate.Update", args, args, reply); done {
		return err
	}

	if !args.Coord.IsValid() {
		return fmt.Errorf("invalid coordinate")
	}

	coord, err := c.srv.serfLAN.GetCoordinate()
	if err != nil {
		return err
	}
	if !coord.IsCompatibleWith(args.Coord) {
		return fmt.Errorf("incompatible coordinate")
	}

	rule, err := c.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	if rule != nil && c.srv.config.ACLEnforceVersion8 {
		if !rule.NodeWrite(args.Node, nil) {
			return acl.ErrPermissionDenied
		}
	}

	key := fmt.Sprintf("%s:%s", args.Node, args.Segment)
	c.updatesLock.Lock()
	c.updates[key] = args
	c.updatesLock.Unlock()
	return nil
}

func (c *Coordinate) ListDatacenters(args *struct{}, reply *[]structs.DatacenterMap) error {
	maps, err := c.srv.router.GetDatacenterMaps()
	if err != nil {
		return err
	}

	for i := range maps {
		suffix := fmt.Sprintf(".%s", maps[i].Datacenter)
		for j := range maps[i].Coordinates {
			node := maps[i].Coordinates[j].Node
			maps[i].Coordinates[j].Node = strings.TrimSuffix(node, suffix)
		}
	}

	*reply = maps
	return nil
}

func (c *Coordinate) ListNodes(args *structs.DCSpecificRequest, reply *structs.IndexedCoordinates) error {
	if done, err := c.srv.forward("Coordinate.ListNodes", args, args, reply); done {
		return err
	}

	return c.srv.blockingQuery(&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, coords, err := state.Coordinates(ws)
			if err != nil {
				return err
			}

			reply.Index, reply.Coordinates = index, coords
			if err := c.srv.filterACL(args.Token, reply); err != nil {
				return err
			}

			return nil
		})
}

func (c *Coordinate) Node(args *structs.NodeSpecificRequest, reply *structs.IndexedCoordinates) error {
	if done, err := c.srv.forward("Coordinate.Node", args, args, reply); done {
		return err
	}

	rule, err := c.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	if rule != nil && c.srv.config.ACLEnforceVersion8 {
		if !rule.NodeRead(args.Node) {
			return acl.ErrPermissionDenied
		}
	}

	return c.srv.blockingQuery(&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, nodeCoords, err := state.Coordinate(args.Node, ws)
			if err != nil {
				return err
			}

			var coords structs.Coordinates
			for segment, coord := range nodeCoords {
				coords = append(coords, &structs.Coordinate{
					Node:    args.Node,
					Segment: segment,
					Coord:   coord,
				})
			}
			reply.Index, reply.Coordinates = index, coords
			return nil
		})
}
