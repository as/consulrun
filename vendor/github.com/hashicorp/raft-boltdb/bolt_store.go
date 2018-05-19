package raftboltdb

import (
	"errors"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
)

const (
	dbFileMode = 0600
)

var (
	dbLogs = []byte("logs")
	dbConf = []byte("conf")

	ErrKeyNotFound = errors.New("not found")
)

type BoltStore struct {
	conn *bolt.DB

	path string
}

func NewBoltStore(path string) (*BoltStore, error) {

	handle, err := bolt.Open(path, dbFileMode, nil)
	if err != nil {
		return nil, err
	}

	store := &BoltStore{
		conn: handle,
		path: path,
	}

	if err := store.initialize(); err != nil {
		store.Close()
		return nil, err
	}

	return store, nil
}

func (b *BoltStore) initialize() error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.CreateBucketIfNotExists(dbLogs); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(dbConf); err != nil {
		return err
	}

	return tx.Commit()
}

func (b *BoltStore) Close() error {
	return b.conn.Close()
}

func (b *BoltStore) FirstIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	if first, _ := curs.First(); first == nil {
		return 0, nil
	} else {
		return bytesToUint64(first), nil
	}
}

func (b *BoltStore) LastIndex() (uint64, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	if last, _ := curs.Last(); last == nil {
		return 0, nil
	} else {
		return bytesToUint64(last), nil
	}
}

func (b *BoltStore) GetLog(idx uint64, log *raft.Log) error {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbLogs)
	val := bucket.Get(uint64ToBytes(idx))

	if val == nil {
		return raft.ErrLogNotFound
	}
	return decodeMsgPack(val, log)
}

func (b *BoltStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

func (b *BoltStore) StoreLogs(logs []*raft.Log) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, log := range logs {
		key := uint64ToBytes(log.Index)
		val, err := encodeMsgPack(log)
		if err != nil {
			return err
		}
		bucket := tx.Bucket(dbLogs)
		if err := bucket.Put(key, val.Bytes()); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (b *BoltStore) DeleteRange(min, max uint64) error {
	minKey := uint64ToBytes(min)

	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	curs := tx.Bucket(dbLogs).Cursor()
	for k, _ := curs.Seek(minKey); k != nil; k, _ = curs.Next() {

		if bytesToUint64(k) > max {
			break
		}

		if err := curs.Delete(); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (b *BoltStore) Set(k, v []byte) error {
	tx, err := b.conn.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbConf)
	if err := bucket.Put(k, v); err != nil {
		return err
	}

	return tx.Commit()
}

func (b *BoltStore) Get(k []byte) ([]byte, error) {
	tx, err := b.conn.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(dbConf)
	val := bucket.Get(k)

	if val == nil {
		return nil, ErrKeyNotFound
	}
	return append([]byte{}, val...), nil
}

func (b *BoltStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

func (b *BoltStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}
