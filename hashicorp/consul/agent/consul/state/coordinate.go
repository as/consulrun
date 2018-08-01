package state

import (
	"fmt"

	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/lib"
	"github.com/as/consulrun/hashicorp/go-memdb"
)

func coordinatesTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "coordinates",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.CompoundIndex{

					AllowMissing: true,
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field:     "Node",
							Lowercase: true,
						},
						&memdb.StringFieldIndex{
							Field:     "Segment",
							Lowercase: true,
						},
					},
				},
			},
			"node": {
				Name:         "node",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Node",
					Lowercase: true,
				},
			},
		},
	}
}

func init() {
	registerSchema(coordinatesTableSchema)
}

func (s *Snapshot) Coordinates() (memdb.ResultIterator, error) {
	iter, err := s.tx.Get("coordinates", "id")
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (s *Restore) Coordinates(idx uint64, updates structs.Coordinates) error {
	for _, update := range updates {

		if !update.Coord.IsValid() {
			continue
		}

		if err := s.tx.Insert("coordinates", update); err != nil {
			return fmt.Errorf("failed restoring coordinate: %s", err)
		}
	}

	if err := indexUpdateMaxTxn(s.tx, idx, "coordinates"); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

func (s *Store) Coordinate(node string, ws memdb.WatchSet) (uint64, lib.CoordinateSet, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	tableIdx := maxIndexTxn(tx, "coordinates")

	iter, err := tx.Get("coordinates", "node", node)
	if err != nil {
		return 0, nil, fmt.Errorf("failed coordinate lookup: %s", err)
	}
	ws.Add(iter.WatchCh())

	results := make(lib.CoordinateSet)
	for raw := iter.Next(); raw != nil; raw = iter.Next() {
		coord := raw.(*structs.Coordinate)
		results[coord.Segment] = coord.Coord
	}
	return tableIdx, results, nil
}

func (s *Store) Coordinates(ws memdb.WatchSet) (uint64, structs.Coordinates, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "coordinates")

	iter, err := tx.Get("coordinates", "id")
	if err != nil {
		return 0, nil, fmt.Errorf("failed coordinate lookup: %s", err)
	}
	ws.Add(iter.WatchCh())

	var results structs.Coordinates
	for coord := iter.Next(); coord != nil; coord = iter.Next() {
		results = append(results, coord.(*structs.Coordinate))
	}
	return idx, results, nil
}

func (s *Store) CoordinateBatchUpdate(idx uint64, updates structs.Coordinates) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	for _, update := range updates {

		if !update.Coord.IsValid() {
			continue
		}

		node, err := tx.First("nodes", "id", update.Node)
		if err != nil {
			return fmt.Errorf("failed node lookup: %s", err)
		}
		if node == nil {
			continue
		}

		if err := tx.Insert("coordinates", update); err != nil {
			return fmt.Errorf("failed inserting coordinate: %s", err)
		}
	}

	if err := tx.Insert("index", &IndexEntry{"coordinates", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	tx.Commit()
	return nil
}
