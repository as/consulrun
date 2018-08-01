package memdb

import (
	"bytes"
	"fmt"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/as/consulrun/hashicorp/go-immutable-radix"
)

const (
	id = "id"
)

type tableIndex struct {
	Table string
	Index string
}

type Txn struct {
	db      *MemDB
	write   bool
	rootTxn *iradix.Txn
	after   []func()

	modified map[tableIndex]*iradix.Txn
}

func (txn *Txn) readableIndex(table, index string) *iradix.Txn {

	if txn.write && txn.modified != nil {
		key := tableIndex{table, index}
		exist, ok := txn.modified[key]
		if ok {
			return exist
		}
	}

	path := indexPath(table, index)
	raw, _ := txn.rootTxn.Get(path)
	indexTxn := raw.(*iradix.Tree).Txn()
	return indexTxn
}

func (txn *Txn) writableIndex(table, index string) *iradix.Txn {
	if txn.modified == nil {
		txn.modified = make(map[tableIndex]*iradix.Txn)
	}

	key := tableIndex{table, index}
	exist, ok := txn.modified[key]
	if ok {
		return exist
	}

	path := indexPath(table, index)
	raw, _ := txn.rootTxn.Get(path)
	indexTxn := raw.(*iradix.Tree).Txn()

	indexTxn.TrackMutate(txn.db.primary)

	txn.modified[key] = indexTxn
	return indexTxn
}

func (txn *Txn) Abort() {

	if !txn.write {
		return
	}

	if txn.rootTxn == nil {
		return
	}

	txn.rootTxn = nil
	txn.modified = nil

	txn.db.writer.Unlock()
}

func (txn *Txn) Commit() {

	if !txn.write {
		return
	}

	if txn.rootTxn == nil {
		return
	}

	for key, subTxn := range txn.modified {
		path := indexPath(key.Table, key.Index)
		final := subTxn.CommitOnly()
		txn.rootTxn.Insert(path, final)
	}

	newRoot := txn.rootTxn.CommitOnly()
	atomic.StorePointer(&txn.db.root, unsafe.Pointer(newRoot))

	for _, subTxn := range txn.modified {
		subTxn.Notify()
	}
	txn.rootTxn.Notify()

	txn.rootTxn = nil
	txn.modified = nil

	txn.db.writer.Unlock()

	for i := len(txn.after); i > 0; i-- {
		fn := txn.after[i-1]
		fn()
	}
}

func (txn *Txn) Insert(table string, obj interface{}) error {
	if !txn.write {
		return fmt.Errorf("cannot insert in read-only transaction")
	}

	tableSchema, ok := txn.db.schema.Tables[table]
	if !ok {
		return fmt.Errorf("invalid table '%s'", table)
	}

	idSchema := tableSchema.Indexes[id]
	idIndexer := idSchema.Indexer.(SingleIndexer)
	ok, idVal, err := idIndexer.FromObject(obj)
	if err != nil {
		return fmt.Errorf("failed to build primary index: %v", err)
	}
	if !ok {
		return fmt.Errorf("object missing primary index")
	}

	idTxn := txn.writableIndex(table, id)
	existing, update := idTxn.Get(idVal)

	for name, indexSchema := range tableSchema.Indexes {
		indexTxn := txn.writableIndex(table, name)

		var (
			ok   bool
			vals [][]byte
			err  error
		)
		switch indexer := indexSchema.Indexer.(type) {
		case SingleIndexer:
			var val []byte
			ok, val, err = indexer.FromObject(obj)
			vals = [][]byte{val}
		case MultiIndexer:
			ok, vals, err = indexer.FromObject(obj)
		}
		if err != nil {
			return fmt.Errorf("failed to build index '%s': %v", name, err)
		}

		if ok && !indexSchema.Unique {
			for i := range vals {
				vals[i] = append(vals[i], idVal...)
			}
		}

		if update {
			var (
				okExist   bool
				valsExist [][]byte
				err       error
			)
			switch indexer := indexSchema.Indexer.(type) {
			case SingleIndexer:
				var valExist []byte
				okExist, valExist, err = indexer.FromObject(existing)
				valsExist = [][]byte{valExist}
			case MultiIndexer:
				okExist, valsExist, err = indexer.FromObject(existing)
			}
			if err != nil {
				return fmt.Errorf("failed to build index '%s': %v", name, err)
			}
			if okExist {
				for i, valExist := range valsExist {

					if !indexSchema.Unique {
						valExist = append(valExist, idVal...)
					}

					if i >= len(vals) || !bytes.Equal(valExist, vals[i]) {
						indexTxn.Delete(valExist)
					}
				}
			}
		}

		if !ok {
			if indexSchema.AllowMissing {
				continue
			} else {
				return fmt.Errorf("missing value for index '%s'", name)
			}
		}

		for _, val := range vals {
			indexTxn.Insert(val, obj)
		}
	}
	return nil
}

func (txn *Txn) Delete(table string, obj interface{}) error {
	if !txn.write {
		return fmt.Errorf("cannot delete in read-only transaction")
	}

	tableSchema, ok := txn.db.schema.Tables[table]
	if !ok {
		return fmt.Errorf("invalid table '%s'", table)
	}

	idSchema := tableSchema.Indexes[id]
	idIndexer := idSchema.Indexer.(SingleIndexer)
	ok, idVal, err := idIndexer.FromObject(obj)
	if err != nil {
		return fmt.Errorf("failed to build primary index: %v", err)
	}
	if !ok {
		return fmt.Errorf("object missing primary index")
	}

	idTxn := txn.writableIndex(table, id)
	existing, ok := idTxn.Get(idVal)
	if !ok {
		return fmt.Errorf("not found")
	}

	for name, indexSchema := range tableSchema.Indexes {
		indexTxn := txn.writableIndex(table, name)

		var (
			ok   bool
			vals [][]byte
			err  error
		)
		switch indexer := indexSchema.Indexer.(type) {
		case SingleIndexer:
			var val []byte
			ok, val, err = indexer.FromObject(existing)
			vals = [][]byte{val}
		case MultiIndexer:
			ok, vals, err = indexer.FromObject(existing)
		}
		if err != nil {
			return fmt.Errorf("failed to build index '%s': %v", name, err)
		}
		if ok {

			for _, val := range vals {
				if !indexSchema.Unique {
					val = append(val, idVal...)
				}
				indexTxn.Delete(val)
			}
		}
	}
	return nil
}

func (txn *Txn) DeletePrefix(table string, prefix_index string, prefix string) (bool, error) {
	if !txn.write {
		return false, fmt.Errorf("cannot delete in read-only transaction")
	}

	if !strings.HasSuffix(prefix_index, "_prefix") {
		return false, fmt.Errorf("Index name for DeletePrefix must be a prefix index, Got %v ", prefix_index)
	}

	deletePrefixIndex := strings.TrimSuffix(prefix_index, "_prefix")

	entries, err := txn.Get(table, prefix_index, prefix)
	if err != nil {
		return false, fmt.Errorf("failed kvs lookup: %s", err)
	}

	tableSchema, ok := txn.db.schema.Tables[table]
	if !ok {
		return false, fmt.Errorf("invalid table '%s'", table)
	}

	foundAny := false
	for entry := entries.Next(); entry != nil; entry = entries.Next() {
		if !foundAny {
			foundAny = true
		}

		idSchema := tableSchema.Indexes[id]
		idIndexer := idSchema.Indexer.(SingleIndexer)
		ok, idVal, err := idIndexer.FromObject(entry)
		if err != nil {
			return false, fmt.Errorf("failed to build primary index: %v", err)
		}
		if !ok {
			return false, fmt.Errorf("object missing primary index")
		}

		for name, indexSchema := range tableSchema.Indexes {
			if name == deletePrefixIndex {
				continue
			}
			indexTxn := txn.writableIndex(table, name)

			var (
				ok   bool
				vals [][]byte
				err  error
			)
			switch indexer := indexSchema.Indexer.(type) {
			case SingleIndexer:
				var val []byte
				ok, val, err = indexer.FromObject(entry)
				vals = [][]byte{val}
			case MultiIndexer:
				ok, vals, err = indexer.FromObject(entry)
			}
			if err != nil {
				return false, fmt.Errorf("failed to build index '%s': %v", name, err)
			}

			if ok {

				for _, val := range vals {
					if !indexSchema.Unique {
						val = append(val, idVal...)
					}
					indexTxn.Delete(val)
				}
			}
		}
	}
	if foundAny {
		indexTxn := txn.writableIndex(table, deletePrefixIndex)
		ok = indexTxn.DeletePrefix([]byte(prefix))
		if !ok {
			panic(fmt.Errorf("prefix %v matched some entries but DeletePrefix did not delete any ", prefix))
		}
		return true, nil
	}
	return false, nil
}

func (txn *Txn) DeleteAll(table, index string, args ...interface{}) (int, error) {
	if !txn.write {
		return 0, fmt.Errorf("cannot delete in read-only transaction")
	}

	iter, err := txn.Get(table, index, args...)
	if err != nil {
		return 0, err
	}

	var objs []interface{}
	for {
		obj := iter.Next()
		if obj == nil {
			break
		}

		objs = append(objs, obj)
	}

	num := 0
	for _, obj := range objs {
		if err := txn.Delete(table, obj); err != nil {
			return num, err
		}
		num++
	}
	return num, nil
}

func (txn *Txn) FirstWatch(table, index string, args ...interface{}) (<-chan struct{}, interface{}, error) {

	indexSchema, val, err := txn.getIndexValue(table, index, args...)
	if err != nil {
		return nil, nil, err
	}

	indexTxn := txn.readableIndex(table, indexSchema.Name)

	if indexSchema.Unique && val != nil && indexSchema.Name == index {
		watch, obj, ok := indexTxn.GetWatch(val)
		if !ok {
			return watch, nil, nil
		}
		return watch, obj, nil
	}

	iter := indexTxn.Root().Iterator()
	watch := iter.SeekPrefixWatch(val)
	_, value, _ := iter.Next()
	return watch, value, nil
}

func (txn *Txn) First(table, index string, args ...interface{}) (interface{}, error) {
	_, val, err := txn.FirstWatch(table, index, args...)
	return val, err
}

func (txn *Txn) LongestPrefix(table, index string, args ...interface{}) (interface{}, error) {

	if !strings.HasSuffix(index, "_prefix") {
		return nil, fmt.Errorf("must use '%s_prefix' on index", index)
	}

	indexSchema, val, err := txn.getIndexValue(table, index, args...)
	if err != nil {
		return nil, err
	}

	if !indexSchema.Unique {
		return nil, fmt.Errorf("index '%s' is not unique", index)
	}

	indexTxn := txn.readableIndex(table, indexSchema.Name)
	if _, value, ok := indexTxn.Root().LongestPrefix(val); ok {
		return value, nil
	}
	return nil, nil
}

func (txn *Txn) getIndexValue(table, index string, args ...interface{}) (*IndexSchema, []byte, error) {

	tableSchema, ok := txn.db.schema.Tables[table]
	if !ok {
		return nil, nil, fmt.Errorf("invalid table '%s'", table)
	}

	prefixScan := false
	if strings.HasSuffix(index, "_prefix") {
		index = strings.TrimSuffix(index, "_prefix")
		prefixScan = true
	}

	indexSchema, ok := tableSchema.Indexes[index]
	if !ok {
		return nil, nil, fmt.Errorf("invalid index '%s'", index)
	}

	if len(args) == 0 {
		return indexSchema, nil, nil
	}

	if prefixScan {
		prefixIndexer, ok := indexSchema.Indexer.(PrefixIndexer)
		if !ok {
			return indexSchema, nil,
				fmt.Errorf("index '%s' does not support prefix scanning", index)
		}

		val, err := prefixIndexer.PrefixFromArgs(args...)
		if err != nil {
			return indexSchema, nil, fmt.Errorf("index error: %v", err)
		}
		return indexSchema, val, err
	}

	val, err := indexSchema.Indexer.FromArgs(args...)
	if err != nil {
		return indexSchema, nil, fmt.Errorf("index error: %v", err)
	}
	return indexSchema, val, err
}

type ResultIterator interface {
	WatchCh() <-chan struct{}
	Next() interface{}
}

func (txn *Txn) Get(table, index string, args ...interface{}) (ResultIterator, error) {

	indexSchema, val, err := txn.getIndexValue(table, index, args...)
	if err != nil {
		return nil, err
	}

	indexTxn := txn.readableIndex(table, indexSchema.Name)
	indexRoot := indexTxn.Root()

	indexIter := indexRoot.Iterator()

	watchCh := indexIter.SeekPrefixWatch(val)

	iter := &radixIterator{
		iter:    indexIter,
		watchCh: watchCh,
	}
	return iter, nil
}

// Defer is used to push a new arbitrary function onto a stack which
func (txn *Txn) Defer(fn func()) {
	txn.after = append(txn.after, fn)
}

type radixIterator struct {
	iter    *iradix.Iterator
	watchCh <-chan struct{}
}

func (r *radixIterator) WatchCh() <-chan struct{} {
	return r.watchCh
}

func (r *radixIterator) Next() interface{} {
	_, value, ok := r.iter.Next()
	if !ok {
		return nil
	}
	return value
}
