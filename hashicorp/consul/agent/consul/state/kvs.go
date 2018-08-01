package state

import (
	"fmt"
	"strings"
	"time"

	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/go-memdb"
)

func kvsTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "kvs",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Key",
					Lowercase: false,
				},
			},
			"session": {
				Name:         "session",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.UUIDFieldIndex{
					Field: "Session",
				},
			},
		},
	}
}

func tombstonesTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "tombstones",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Key",
					Lowercase: false,
				},
			},
		},
	}
}

func init() {
	registerSchema(kvsTableSchema)
	registerSchema(tombstonesTableSchema)
}

func (s *Snapshot) KVs() (memdb.ResultIterator, error) {
	iter, err := s.tx.Get("kvs", "id_prefix")
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (s *Snapshot) Tombstones() (memdb.ResultIterator, error) {
	return s.store.kvsGraveyard.DumpTxn(s.tx)
}

func (s *Restore) KVS(entry *structs.DirEntry) error {
	if err := s.tx.Insert("kvs", entry); err != nil {
		return fmt.Errorf("failed inserting kvs entry: %s", err)
	}

	if err := indexUpdateMaxTxn(s.tx, entry.ModifyIndex, "kvs"); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}
	return nil
}

func (s *Restore) Tombstone(stone *Tombstone) error {
	if err := s.store.kvsGraveyard.RestoreTxn(s.tx, stone); err != nil {
		return fmt.Errorf("failed restoring tombstone: %s", err)
	}
	return nil
}

func (s *Store) ReapTombstones(index uint64) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.kvsGraveyard.ReapTxn(tx, index); err != nil {
		return fmt.Errorf("failed to reap kvs tombstones: %s", err)
	}

	tx.Commit()
	return nil
}

func (s *Store) KVSSet(idx uint64, entry *structs.DirEntry) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.kvsSetTxn(tx, idx, entry, false); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) kvsSetTxn(tx *memdb.Txn, idx uint64, entry *structs.DirEntry, updateSession bool) error {

	existing, err := tx.First("kvs", "id", entry.Key)
	if err != nil {
		return fmt.Errorf("failed kvs lookup: %s", err)
	}

	if existing != nil {
		entry.CreateIndex = existing.(*structs.DirEntry).CreateIndex
	} else {
		entry.CreateIndex = idx
	}
	entry.ModifyIndex = idx

	if !updateSession {
		if existing != nil {
			entry.Session = existing.(*structs.DirEntry).Session
		} else {
			entry.Session = ""
		}
	}

	if err := tx.Insert("kvs", entry); err != nil {
		return fmt.Errorf("failed inserting kvs entry: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{"kvs", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

func (s *Store) KVSGet(ws memdb.WatchSet, key string) (uint64, *structs.DirEntry, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	return s.kvsGetTxn(tx, ws, key)
}

func (s *Store) kvsGetTxn(tx *memdb.Txn, ws memdb.WatchSet, key string) (uint64, *structs.DirEntry, error) {

	idx := maxIndexTxn(tx, "kvs", "tombstones")

	watchCh, entry, err := tx.FirstWatch("kvs", "id", key)
	if err != nil {
		return 0, nil, fmt.Errorf("failed kvs lookup: %s", err)
	}
	ws.Add(watchCh)
	if entry != nil {
		return idx, entry.(*structs.DirEntry), nil
	}
	return idx, nil, nil
}

func (s *Store) KVSList(ws memdb.WatchSet, prefix string) (uint64, structs.DirEntries, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	return s.kvsListTxn(tx, ws, prefix)
}

func (s *Store) kvsListTxn(tx *memdb.Txn, ws memdb.WatchSet, prefix string) (uint64, structs.DirEntries, error) {

	idx := maxIndexTxn(tx, "kvs", "tombstones")

	entries, err := tx.Get("kvs", "id_prefix", prefix)
	if err != nil {
		return 0, nil, fmt.Errorf("failed kvs lookup: %s", err)
	}
	ws.Add(entries.WatchCh())

	var ents structs.DirEntries
	var lindex uint64
	for entry := entries.Next(); entry != nil; entry = entries.Next() {
		e := entry.(*structs.DirEntry)
		ents = append(ents, e)
		if e.ModifyIndex > lindex {
			lindex = e.ModifyIndex
		}
	}

	if prefix != "" {
		gindex, err := s.kvsGraveyard.GetMaxIndexTxn(tx, prefix)
		if err != nil {
			return 0, nil, fmt.Errorf("failed graveyard lookup: %s", err)
		}
		if gindex > lindex {
			lindex = gindex
		}
	} else {
		lindex = idx
	}

	if lindex != 0 {
		idx = lindex
	}
	return idx, ents, nil
}

func (s *Store) KVSListKeys(ws memdb.WatchSet, prefix, sep string) (uint64, []string, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "kvs", "tombstones")

	entries, err := tx.Get("kvs", "id_prefix", prefix)
	if err != nil {
		return 0, nil, fmt.Errorf("failed kvs lookup: %s", err)
	}
	ws.Add(entries.WatchCh())

	prefixLen := len(prefix)
	sepLen := len(sep)

	var keys []string
	var lindex uint64
	var last string
	for entry := entries.Next(); entry != nil; entry = entries.Next() {
		e := entry.(*structs.DirEntry)

		if e.ModifyIndex > lindex {
			lindex = e.ModifyIndex
		}

		if sepLen == 0 {
			keys = append(keys, e.Key)
			continue
		}

		after := e.Key[prefixLen:]
		sepIdx := strings.Index(after, sep)
		if sepIdx > -1 {
			key := e.Key[:prefixLen+sepIdx+sepLen]
			if key != last {
				keys = append(keys, key)
				last = key
			}
		} else {
			keys = append(keys, e.Key)
		}
	}

	if prefix != "" {
		gindex, err := s.kvsGraveyard.GetMaxIndexTxn(tx, prefix)
		if err != nil {
			return 0, nil, fmt.Errorf("failed graveyard lookup: %s", err)
		}
		if gindex > lindex {
			lindex = gindex
		}
	} else {
		lindex = idx
	}

	if lindex != 0 {
		idx = lindex
	}
	return idx, keys, nil
}

func (s *Store) KVSDelete(idx uint64, key string) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.kvsDeleteTxn(tx, idx, key); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) kvsDeleteTxn(tx *memdb.Txn, idx uint64, key string) error {

	entry, err := tx.First("kvs", "id", key)
	if err != nil {
		return fmt.Errorf("failed kvs lookup: %s", err)
	}
	if entry == nil {
		return nil
	}

	if err := s.kvsGraveyard.InsertTxn(tx, key, idx); err != nil {
		return fmt.Errorf("failed adding to graveyard: %s", err)
	}

	if err := tx.Delete("kvs", entry); err != nil {
		return fmt.Errorf("failed deleting kvs entry: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{"kvs", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

func (s *Store) KVSDeleteCAS(idx, cidx uint64, key string) (bool, error) {
	tx := s.db.Txn(true)
	defer tx.Abort()

	set, err := s.kvsDeleteCASTxn(tx, idx, cidx, key)
	if !set || err != nil {
		return false, err
	}

	tx.Commit()
	return true, nil
}

func (s *Store) kvsDeleteCASTxn(tx *memdb.Txn, idx, cidx uint64, key string) (bool, error) {

	entry, err := tx.First("kvs", "id", key)
	if err != nil {
		return false, fmt.Errorf("failed kvs lookup: %s", err)
	}

	e, ok := entry.(*structs.DirEntry)
	if !ok || e.ModifyIndex != cidx {
		return entry == nil, nil
	}

	if err := s.kvsDeleteTxn(tx, idx, key); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) KVSSetCAS(idx uint64, entry *structs.DirEntry) (bool, error) {
	tx := s.db.Txn(true)
	defer tx.Abort()

	set, err := s.kvsSetCASTxn(tx, idx, entry)
	if !set || err != nil {
		return false, err
	}

	tx.Commit()
	return true, nil
}

func (s *Store) kvsSetCASTxn(tx *memdb.Txn, idx uint64, entry *structs.DirEntry) (bool, error) {

	existing, err := tx.First("kvs", "id", entry.Key)
	if err != nil {
		return false, fmt.Errorf("failed kvs lookup: %s", err)
	}

	if entry.ModifyIndex == 0 && existing != nil {
		return false, nil
	}
	if entry.ModifyIndex != 0 && existing == nil {
		return false, nil
	}
	e, ok := existing.(*structs.DirEntry)
	if ok && entry.ModifyIndex != 0 && entry.ModifyIndex != e.ModifyIndex {
		return false, nil
	}

	if err := s.kvsSetTxn(tx, idx, entry, false); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) KVSDeleteTree(idx uint64, prefix string) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.kvsDeleteTreeTxn(tx, idx, prefix); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) kvsDeleteTreeTxn(tx *memdb.Txn, idx uint64, prefix string) error {

	deleted, err := tx.DeletePrefix("kvs", "id_prefix", prefix)

	if err != nil {
		return fmt.Errorf("failed recursive deleting kvs entry: %s", err)
	}

	if deleted {
		if prefix != "" { // don't insert a tombstone if the entire tree is deleted, all watchers on keys will see the max_index of the tree
			if err := s.kvsGraveyard.InsertTxn(tx, prefix, idx); err != nil {
				return fmt.Errorf("failed adding to graveyard: %s", err)
			}
		}
		if err := tx.Insert("index", &IndexEntry{"kvs", idx}); err != nil {
			return fmt.Errorf("failed updating index: %s", err)
		}
	}
	return nil
}

func (s *Store) KVSLockDelay(key string) time.Time {
	return s.lockDelay.GetExpiration(key)
}

func (s *Store) KVSLock(idx uint64, entry *structs.DirEntry) (bool, error) {
	tx := s.db.Txn(true)
	defer tx.Abort()

	locked, err := s.kvsLockTxn(tx, idx, entry)
	if !locked || err != nil {
		return false, err
	}

	tx.Commit()
	return true, nil
}

func (s *Store) kvsLockTxn(tx *memdb.Txn, idx uint64, entry *structs.DirEntry) (bool, error) {

	if entry.Session == "" {
		return false, fmt.Errorf("missing session")
	}

	sess, err := tx.First("sessions", "id", entry.Session)
	if err != nil {
		return false, fmt.Errorf("failed session lookup: %s", err)
	}
	if sess == nil {
		return false, fmt.Errorf("invalid session %#v", entry.Session)
	}

	existing, err := tx.First("kvs", "id", entry.Key)
	if err != nil {
		return false, fmt.Errorf("failed kvs lookup: %s", err)
	}

	if existing != nil {
		e := existing.(*structs.DirEntry)
		if e.Session == entry.Session {

			entry.CreateIndex = e.CreateIndex
			entry.LockIndex = e.LockIndex
		} else if e.Session != "" {

			return false, nil
		} else {

			entry.CreateIndex = e.CreateIndex
			entry.LockIndex = e.LockIndex + 1
		}
	} else {
		entry.CreateIndex = idx
		entry.LockIndex = 1
	}
	entry.ModifyIndex = idx

	if err := s.kvsSetTxn(tx, idx, entry, true); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) KVSUnlock(idx uint64, entry *structs.DirEntry) (bool, error) {
	tx := s.db.Txn(true)
	defer tx.Abort()

	unlocked, err := s.kvsUnlockTxn(tx, idx, entry)
	if !unlocked || err != nil {
		return false, err
	}

	tx.Commit()
	return true, nil
}

func (s *Store) kvsUnlockTxn(tx *memdb.Txn, idx uint64, entry *structs.DirEntry) (bool, error) {

	if entry.Session == "" {
		return false, fmt.Errorf("missing session")
	}

	existing, err := tx.First("kvs", "id", entry.Key)
	if err != nil {
		return false, fmt.Errorf("failed kvs lookup: %s", err)
	}

	if existing == nil {
		return false, nil
	}

	e := existing.(*structs.DirEntry)
	if e.Session != entry.Session {
		return false, nil
	}

	entry.Session = ""
	entry.LockIndex = e.LockIndex
	entry.CreateIndex = e.CreateIndex
	entry.ModifyIndex = idx

	if err := s.kvsSetTxn(tx, idx, entry, true); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) kvsCheckSessionTxn(tx *memdb.Txn, key string, session string) (*structs.DirEntry, error) {
	entry, err := tx.First("kvs", "id", key)
	if err != nil {
		return nil, fmt.Errorf("failed kvs lookup: %s", err)
	}
	if entry == nil {
		return nil, fmt.Errorf("failed to check session, key %q doesn't exist", key)
	}

	e := entry.(*structs.DirEntry)
	if e.Session != session {
		return nil, fmt.Errorf("failed session check for key %q, current session %q != %q", key, e.Session, session)
	}

	return e, nil
}

func (s *Store) kvsCheckIndexTxn(tx *memdb.Txn, key string, cidx uint64) (*structs.DirEntry, error) {
	entry, err := tx.First("kvs", "id", key)
	if err != nil {
		return nil, fmt.Errorf("failed kvs lookup: %s", err)
	}
	if entry == nil {
		return nil, fmt.Errorf("failed to check index, key %q doesn't exist", key)
	}

	e := entry.(*structs.DirEntry)
	if e.ModifyIndex != cidx {
		return nil, fmt.Errorf("failed index check for key %q, current modify index %d != %d", key, e.ModifyIndex, cidx)
	}

	return e, nil
}
