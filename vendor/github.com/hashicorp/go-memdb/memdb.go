package memdb

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/hashicorp/go-immutable-radix"
)

type MemDB struct {
	schema  *DBSchema
	root    unsafe.Pointer // *iradix.Tree underneath
	primary bool

	writer sync.Mutex
}

func NewMemDB(schema *DBSchema) (*MemDB, error) {

	if err := schema.Validate(); err != nil {
		return nil, err
	}

	db := &MemDB{
		schema:  schema,
		root:    unsafe.Pointer(iradix.New()),
		primary: true,
	}
	if err := db.initialize(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *MemDB) getRoot() *iradix.Tree {
	root := (*iradix.Tree)(atomic.LoadPointer(&db.root))
	return root
}

func (db *MemDB) Txn(write bool) *Txn {
	if write {
		db.writer.Lock()
	}
	txn := &Txn{
		db:      db,
		write:   write,
		rootTxn: db.getRoot().Txn(),
	}
	return txn
}

func (db *MemDB) Snapshot() *MemDB {
	clone := &MemDB{
		schema:  db.schema,
		root:    unsafe.Pointer(db.getRoot()),
		primary: false,
	}
	return clone
}

func (db *MemDB) initialize() error {
	root := db.getRoot()
	for tName, tableSchema := range db.schema.Tables {
		for iName := range tableSchema.Indexes {
			index := iradix.New()
			path := indexPath(tName, iName)
			root, _, _ = root.Insert(path, index)
		}
	}
	db.root = unsafe.Pointer(root)
	return nil
}

func indexPath(table, index string) []byte {
	return []byte(table + "." + index)
}
