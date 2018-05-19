package state

import (
	"fmt"

	"github.com/hashicorp/go-memdb"
)

type Tombstone struct {
	Key   string
	Index uint64
}

type Graveyard struct {
	gc *TombstoneGC
}

func NewGraveyard(gc *TombstoneGC) *Graveyard {
	return &Graveyard{gc: gc}
}

func (g *Graveyard) InsertTxn(tx *memdb.Txn, key string, idx uint64) error {

	stone := &Tombstone{Key: key, Index: idx}
	if err := tx.Insert("tombstones", stone); err != nil {
		return fmt.Errorf("failed inserting tombstone: %s", err)
	}

	if err := tx.Insert("index", &IndexEntry{"tombstones", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	if g.gc != nil {
		tx.Defer(func() { g.gc.Hint(idx) })
	}
	return nil
}

func (g *Graveyard) GetMaxIndexTxn(tx *memdb.Txn, prefix string) (uint64, error) {
	stones, err := tx.Get("tombstones", "id_prefix", prefix)
	if err != nil {
		return 0, fmt.Errorf("failed querying tombstones: %s", err)
	}

	var lindex uint64
	for stone := stones.Next(); stone != nil; stone = stones.Next() {
		s := stone.(*Tombstone)
		if s.Index > lindex {
			lindex = s.Index
		}
	}
	return lindex, nil
}

func (g *Graveyard) DumpTxn(tx *memdb.Txn) (memdb.ResultIterator, error) {
	iter, err := tx.Get("tombstones", "id")
	if err != nil {
		return nil, err
	}

	return iter, nil
}

func (g *Graveyard) RestoreTxn(tx *memdb.Txn, stone *Tombstone) error {
	if err := tx.Insert("tombstones", stone); err != nil {
		return fmt.Errorf("failed inserting tombstone: %s", err)
	}

	if err := indexUpdateMaxTxn(tx, stone.Index, "tombstones"); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}
	return nil
}

func (g *Graveyard) ReapTxn(tx *memdb.Txn, idx uint64) error {

	stones, err := tx.Get("tombstones", "id")
	if err != nil {
		return fmt.Errorf("failed querying tombstones: %s", err)
	}

	var objs []interface{}
	for stone := stones.Next(); stone != nil; stone = stones.Next() {
		if stone.(*Tombstone).Index <= idx {
			objs = append(objs, stone)
		}
	}

	for _, obj := range objs {
		if err := tx.Delete("tombstones", obj); err != nil {
			return fmt.Errorf("failed deleting tombstone: %s", err)
		}
	}
	return nil
}
