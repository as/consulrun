package state

import (
	"errors"
	"fmt"

	"github.com/hashicorp/consul/types"
	"github.com/hashicorp/go-memdb"
)

var (
	ErrMissingNode = errors.New("Missing node registration")

	ErrMissingService = errors.New("Missing service registration")

	ErrMissingSessionID = errors.New("Missing session ID")

	ErrMissingACLID = errors.New("Missing ACL ID")

	ErrMissingQueryID = errors.New("Missing Query ID")
)

const (

	//

	watchLimit = 2048
)

type Store struct {
	schema *memdb.DBSchema
	db     *memdb.MemDB

	abandonCh chan struct{}

	kvsGraveyard *Graveyard

	lockDelay *Delay
}

type Snapshot struct {
	store     *Store
	tx        *memdb.Txn
	lastIndex uint64
}

type Restore struct {
	store *Store
	tx    *memdb.Txn
}

type IndexEntry struct {
	Key   string
	Value uint64
}

type sessionCheck struct {
	Node    string
	CheckID types.CheckID
	Session string
}

func NewStateStore(gc *TombstoneGC) (*Store, error) {

	schema := stateStoreSchema()
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, fmt.Errorf("Failed setting up state store: %s", err)
	}

	s := &Store{
		schema:       schema,
		db:           db,
		abandonCh:    make(chan struct{}),
		kvsGraveyard: NewGraveyard(gc),
		lockDelay:    NewDelay(),
	}
	return s, nil
}

func (s *Store) Snapshot() *Snapshot {
	tx := s.db.Txn(false)

	var tables []string
	for table := range s.schema.Tables {
		tables = append(tables, table)
	}
	idx := maxIndexTxn(tx, tables...)

	return &Snapshot{s, tx, idx}
}

func (s *Snapshot) LastIndex() uint64 {
	return s.lastIndex
}

func (s *Snapshot) Close() {
	s.tx.Abort()
}

func (s *Store) Restore() *Restore {
	tx := s.db.Txn(true)
	return &Restore{s, tx}
}

func (s *Restore) Abort() {
	s.tx.Abort()
}

func (s *Restore) Commit() {
	s.tx.Commit()
}

func (s *Store) AbandonCh() <-chan struct{} {
	return s.abandonCh
}

func (s *Store) Abandon() {
	close(s.abandonCh)
}

func (s *Store) maxIndex(tables ...string) uint64 {
	tx := s.db.Txn(false)
	defer tx.Abort()
	return maxIndexTxn(tx, tables...)
}

func maxIndexTxn(tx *memdb.Txn, tables ...string) uint64 {
	var lindex uint64
	for _, table := range tables {
		ti, err := tx.First("index", "id", table)
		if err != nil {
			panic(fmt.Sprintf("unknown index: %s err: %s", table, err))
		}
		if idx, ok := ti.(*IndexEntry); ok && idx.Value > lindex {
			lindex = idx.Value
		}
	}
	return lindex
}

func indexUpdateMaxTxn(tx *memdb.Txn, idx uint64, table string) error {
	ti, err := tx.First("index", "id", table)
	if err != nil {
		return fmt.Errorf("failed to retrieve existing index: %s", err)
	}

	if ti == nil {
		if err := tx.Insert("index", &IndexEntry{table, idx}); err != nil {
			return fmt.Errorf("failed updating index %s", err)
		}
	} else if cur, ok := ti.(*IndexEntry); ok && idx > cur.Value {
		if err := tx.Insert("index", &IndexEntry{table, idx}); err != nil {
			return fmt.Errorf("failed updating index %s", err)
		}
	}

	return nil
}
