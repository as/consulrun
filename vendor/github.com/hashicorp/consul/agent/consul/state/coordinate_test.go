package state

import (
	"math"
	"math/rand"
	"testing"

	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/lib"
	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/serf/coordinate"
	"github.com/pascaldekloe/goe/verify"
)

func generateRandomCoordinate() *coordinate.Coordinate {
	config := coordinate.DefaultConfig()
	coord := coordinate.NewCoordinate(config)
	for i := range coord.Vec {
		coord.Vec[i] = rand.NormFloat64()
	}
	coord.Error = rand.NormFloat64()
	coord.Adjustment = rand.NormFloat64()
	return coord
}

func TestStateStore_Coordinate_Updates(t *testing.T) {
	s := testStateStore(t)

	ws := memdb.NewWatchSet()
	idx, all, err := s.Coordinates(ws)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad index: %d", idx)
	}
	verify.Values(t, "", all, structs.Coordinates{})

	coordinateWs := memdb.NewWatchSet()
	_, coords, err := s.Coordinate("nope", coordinateWs)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	verify.Values(t, "", coords, lib.CoordinateSet{})

	updates := structs.Coordinates{
		&structs.Coordinate{
			Node:  "node1",
			Coord: generateRandomCoordinate(),
		},
		&structs.Coordinate{
			Node:  "node2",
			Coord: generateRandomCoordinate(),
		},
	}
	if err := s.CoordinateBatchUpdate(1, updates); err != nil {
		t.Fatalf("err: %s", err)
	}
	if watchFired(ws) || watchFired(coordinateWs) {
		t.Fatalf("bad")
	}

	ws = memdb.NewWatchSet()
	idx, all, err = s.Coordinates(ws)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad index: %d", idx)
	}
	verify.Values(t, "", all, structs.Coordinates{})

	coordinateWs = memdb.NewWatchSet()
	idx, coords, err = s.Coordinate("node1", coordinateWs)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad index: %d", idx)
	}

	testRegisterNode(t, s, 1, "node1")
	testRegisterNode(t, s, 2, "node2")
	if err := s.CoordinateBatchUpdate(3, updates); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !watchFired(ws) || !watchFired(coordinateWs) {
		t.Fatalf("bad")
	}

	ws = memdb.NewWatchSet()
	idx, all, err = s.Coordinates(ws)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}
	verify.Values(t, "", all, updates)

	nodeWs := make([]memdb.WatchSet, len(updates))
	for i, update := range updates {
		nodeWs[i] = memdb.NewWatchSet()
		idx, coords, err := s.Coordinate(update.Node, nodeWs[i])
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if idx != 3 {
			t.Fatalf("bad index: %d", idx)
		}
		expected := lib.CoordinateSet{
			"": update.Coord,
		}
		verify.Values(t, "", coords, expected)
	}

	updates[1].Coord = generateRandomCoordinate()
	if err := s.CoordinateBatchUpdate(4, updates); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}
	for _, ws := range nodeWs {
		if !watchFired(ws) {
			t.Fatalf("bad")
		}
	}

	idx, all, err = s.Coordinates(nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}
	verify.Values(t, "", all, updates)

	for _, update := range updates {
		idx, coords, err := s.Coordinate(update.Node, nil)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if idx != 4 {
			t.Fatalf("bad index: %d", idx)
		}
		expected := lib.CoordinateSet{
			"": update.Coord,
		}
		verify.Values(t, "", coords, expected)
	}

	badUpdates := structs.Coordinates{
		&structs.Coordinate{
			Node:  "node1",
			Coord: &coordinate.Coordinate{Height: math.NaN()},
		},
	}
	if err := s.CoordinateBatchUpdate(5, badUpdates); err != nil {
		t.Fatalf("err: %s", err)
	}

	idx, all, err = s.Coordinates(nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 5 {
		t.Fatalf("bad index: %d", idx)
	}
	verify.Values(t, "", all, updates)
}

func TestStateStore_Coordinate_Cleanup(t *testing.T) {
	s := testStateStore(t)

	testRegisterNode(t, s, 1, "node1")
	updates := structs.Coordinates{
		&structs.Coordinate{
			Node:    "node1",
			Segment: "alpha",
			Coord:   generateRandomCoordinate(),
		},
		&structs.Coordinate{
			Node:    "node1",
			Segment: "beta",
			Coord:   generateRandomCoordinate(),
		},
	}
	if err := s.CoordinateBatchUpdate(2, updates); err != nil {
		t.Fatalf("err: %s", err)
	}

	_, coords, err := s.Coordinate("node1", nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	expected := lib.CoordinateSet{
		"alpha": updates[0].Coord,
		"beta":  updates[1].Coord,
	}
	verify.Values(t, "", coords, expected)

	if err := s.DeleteNode(3, "node1"); err != nil {
		t.Fatalf("err: %s", err)
	}

	_, coords, err = s.Coordinate("node1", nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	verify.Values(t, "", coords, lib.CoordinateSet{})

	idx, all, err := s.Coordinates(nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}
	verify.Values(t, "", all, structs.Coordinates{})
}

func TestStateStore_Coordinate_Snapshot_Restore(t *testing.T) {
	s := testStateStore(t)

	testRegisterNode(t, s, 1, "node1")
	testRegisterNode(t, s, 2, "node2")
	updates := structs.Coordinates{
		&structs.Coordinate{
			Node:  "node1",
			Coord: generateRandomCoordinate(),
		},
		&structs.Coordinate{
			Node:  "node2",
			Coord: generateRandomCoordinate(),
		},
	}
	if err := s.CoordinateBatchUpdate(3, updates); err != nil {
		t.Fatalf("err: %s", err)
	}

	testRegisterNode(t, s, 4, "node3")
	badUpdate := &structs.Coordinate{
		Node:  "node3",
		Coord: &coordinate.Coordinate{Height: math.NaN()},
	}
	tx := s.db.Txn(true)
	if err := tx.Insert("coordinates", badUpdate); err != nil {
		t.Fatalf("err: %v", err)
	}
	tx.Commit()

	snap := s.Snapshot()
	defer snap.Close()

	trash := structs.Coordinates{
		&structs.Coordinate{
			Node:  "node1",
			Coord: generateRandomCoordinate(),
		},
		&structs.Coordinate{
			Node:  "node2",
			Coord: generateRandomCoordinate(),
		},
	}
	if err := s.CoordinateBatchUpdate(5, trash); err != nil {
		t.Fatalf("err: %s", err)
	}

	if idx := snap.LastIndex(); idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}
	iter, err := snap.Coordinates()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	var dump structs.Coordinates
	for coord := iter.Next(); coord != nil; coord = iter.Next() {
		dump = append(dump, coord.(*structs.Coordinate))
	}

	verify.Values(t, "", dump, append(updates, badUpdate))

	func() {
		s := testStateStore(t)
		restore := s.Restore()
		if err := restore.Coordinates(6, dump); err != nil {
			t.Fatalf("err: %s", err)
		}
		restore.Commit()

		idx, res, err := s.Coordinates(nil)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if idx != 6 {
			t.Fatalf("bad index: %d", idx)
		}
		verify.Values(t, "", res, updates)

		if idx := s.maxIndex("coordinates"); idx != 6 {
			t.Fatalf("bad index: %d", idx)
		}
	}()

}
