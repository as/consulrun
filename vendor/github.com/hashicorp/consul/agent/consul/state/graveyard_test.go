package state

import (
	"reflect"
	"testing"
	"time"
)

func TestGraveyard_Lifecycle(t *testing.T) {
	g := NewGraveyard(nil)

	s := testStateStore(t)

	func() {
		tx := s.db.Txn(true)
		defer tx.Abort()

		if err := g.InsertTxn(tx, "foo/in/the/house", 2); err != nil {
			t.Fatalf("err: %s", err)
		}
		if err := g.InsertTxn(tx, "foo/bar/baz", 5); err != nil {
			t.Fatalf("err: %s", err)
		}
		if err := g.InsertTxn(tx, "foo/bar/zoo", 8); err != nil {
			t.Fatalf("err: %s", err)
		}
		if err := g.InsertTxn(tx, "some/other/path", 9); err != nil {
			t.Fatalf("err: %s", err)
		}
		tx.Commit()
	}()

	func() {
		tx := s.db.Txn(false)
		defer tx.Abort()

		if idx, err := g.GetMaxIndexTxn(tx, "foo"); idx != 8 || err != nil {
			t.Fatalf("bad: %d (%s)", idx, err)
		}
		if idx, err := g.GetMaxIndexTxn(tx, "foo/in/the/house"); idx != 2 || err != nil {
			t.Fatalf("bad: %d (%s)", idx, err)
		}
		if idx, err := g.GetMaxIndexTxn(tx, "foo/bar/baz"); idx != 5 || err != nil {
			t.Fatalf("bad: %d (%s)", idx, err)
		}
		if idx, err := g.GetMaxIndexTxn(tx, "foo/bar/zoo"); idx != 8 || err != nil {
			t.Fatalf("bad: %d (%s)", idx, err)
		}
		if idx, err := g.GetMaxIndexTxn(tx, "some/other/path"); idx != 9 || err != nil {
			t.Fatalf("bad: %d (%s)", idx, err)
		}
		if idx, err := g.GetMaxIndexTxn(tx, ""); idx != 9 || err != nil {
			t.Fatalf("bad: %d (%s)", idx, err)
		}
		if idx, err := g.GetMaxIndexTxn(tx, "nope"); idx != 0 || err != nil {
			t.Fatalf("bad: %d (%s)", idx, err)
		}
	}()

	func() {
		tx := s.db.Txn(true)
		defer tx.Abort()

		if err := g.ReapTxn(tx, 6); err != nil {
			t.Fatalf("err: %s", err)
		}
		tx.Commit()
	}()

	func() {
		tx := s.db.Txn(false)
		defer tx.Abort()

		if idx, err := g.GetMaxIndexTxn(tx, "foo"); idx != 8 || err != nil {
			t.Fatalf("bad: %d (%s)", idx, err)
		}
		if idx, err := g.GetMaxIndexTxn(tx, "foo/in/the/house"); idx != 0 || err != nil {
			t.Fatalf("bad: %d (%s)", idx, err)
		}
		if idx, err := g.GetMaxIndexTxn(tx, "foo/bar/baz"); idx != 0 || err != nil {
			t.Fatalf("bad: %d (%s)", idx, err)
		}
		if idx, err := g.GetMaxIndexTxn(tx, "foo/bar/zoo"); idx != 8 || err != nil {
			t.Fatalf("bad: %d (%s)", idx, err)
		}
		if idx, err := g.GetMaxIndexTxn(tx, "some/other/path"); idx != 9 || err != nil {
			t.Fatalf("bad: %d (%s)", idx, err)
		}
		if idx, err := g.GetMaxIndexTxn(tx, ""); idx != 9 || err != nil {
			t.Fatalf("bad: %d (%s)", idx, err)
		}
		if idx, err := g.GetMaxIndexTxn(tx, "nope"); idx != 0 || err != nil {
			t.Fatalf("bad: %d (%s)", idx, err)
		}
	}()
}

func TestGraveyard_GC_Trigger(t *testing.T) {

	ttl, granularity := 100*time.Millisecond, 20*time.Millisecond
	gc, err := NewTombstoneGC(ttl, granularity)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	g := NewGraveyard(gc)
	gc.SetEnabled(true)

	if gc.PendingExpiration() {
		t.Fatalf("should not have any expiring items")
	}

	s := testStateStore(t)
	func() {
		tx := s.db.Txn(true)
		defer tx.Abort()

		if err := g.InsertTxn(tx, "foo/in/the/house", 2); err != nil {
			t.Fatalf("err: %s", err)
		}
	}()

	if gc.PendingExpiration() {
		t.Fatalf("should not have any expiring items")
	}

	func() {
		tx := s.db.Txn(true)
		defer tx.Abort()

		if err := g.InsertTxn(tx, "foo/in/the/house", 2); err != nil {
			t.Fatalf("err: %s", err)
		}
		tx.Commit()
	}()

	if !gc.PendingExpiration() {
		t.Fatalf("should have a pending expiration")
	}

	select {
	case idx := <-gc.ExpireCh():
		if idx != 2 {
			t.Fatalf("bad index: %d", idx)
		}
	case <-time.After(2 * ttl):
		t.Fatalf("should have gotten an expire notice")
	}
}

func TestGraveyard_Snapshot_Restore(t *testing.T) {
	g := NewGraveyard(nil)

	s := testStateStore(t)

	func() {
		tx := s.db.Txn(true)
		defer tx.Abort()

		if err := g.InsertTxn(tx, "foo/in/the/house", 2); err != nil {
			t.Fatalf("err: %s", err)
		}
		if err := g.InsertTxn(tx, "foo/bar/baz", 5); err != nil {
			t.Fatalf("err: %s", err)
		}
		if err := g.InsertTxn(tx, "foo/bar/zoo", 8); err != nil {
			t.Fatalf("err: %s", err)
		}
		if err := g.InsertTxn(tx, "some/other/path", 9); err != nil {
			t.Fatalf("err: %s", err)
		}
		tx.Commit()
	}()

	if idx := s.maxIndex("tombstones"); idx != 9 {
		t.Fatalf("bad index: %d", idx)
	}

	dump := func() []*Tombstone {
		tx := s.db.Txn(false)
		defer tx.Abort()

		iter, err := g.DumpTxn(tx)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		var dump []*Tombstone
		for ti := iter.Next(); ti != nil; ti = iter.Next() {
			dump = append(dump, ti.(*Tombstone))
		}
		return dump
	}()

	expected := []*Tombstone{
		{Key: "foo/bar/baz", Index: 5},
		{Key: "foo/bar/zoo", Index: 8},
		{Key: "foo/in/the/house", Index: 2},
		{Key: "some/other/path", Index: 9},
	}
	if !reflect.DeepEqual(dump, expected) {
		t.Fatalf("bad: %v", dump)
	}

	func() {
		s := testStateStore(t)
		func() {
			tx := s.db.Txn(true)
			defer tx.Abort()

			for _, stone := range dump {
				if err := g.RestoreTxn(tx, stone); err != nil {
					t.Fatalf("err: %s", err)
				}
			}
			tx.Commit()
		}()

		if idx := s.maxIndex("tombstones"); idx != 9 {
			t.Fatalf("bad index: %d", idx)
		}

		dump := func() []*Tombstone {
			tx := s.db.Txn(false)
			defer tx.Abort()

			iter, err := g.DumpTxn(tx)
			if err != nil {
				t.Fatalf("err: %s", err)
			}
			var dump []*Tombstone
			for ti := iter.Next(); ti != nil; ti = iter.Next() {
				dump = append(dump, ti.(*Tombstone))
			}
			return dump
		}()
		if !reflect.DeepEqual(dump, expected) {
			t.Fatalf("bad: %v", dump)
		}
	}()
}
