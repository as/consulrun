package raft

import (
	"sync"
)

type InmemStore struct {
	l         sync.RWMutex
	lowIndex  uint64
	highIndex uint64
	logs      map[uint64]*Log
	kv        map[string][]byte
	kvInt     map[string]uint64
}

func NewInmemStore() *InmemStore {
	i := &InmemStore{
		logs:  make(map[uint64]*Log),
		kv:    make(map[string][]byte),
		kvInt: make(map[string]uint64),
	}
	return i
}

func (i *InmemStore) FirstIndex() (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.lowIndex, nil
}

func (i *InmemStore) LastIndex() (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.highIndex, nil
}

func (i *InmemStore) GetLog(index uint64, log *Log) error {
	i.l.RLock()
	defer i.l.RUnlock()
	l, ok := i.logs[index]
	if !ok {
		return ErrLogNotFound
	}
	*log = *l
	return nil
}

func (i *InmemStore) StoreLog(log *Log) error {
	return i.StoreLogs([]*Log{log})
}

func (i *InmemStore) StoreLogs(logs []*Log) error {
	i.l.Lock()
	defer i.l.Unlock()
	for _, l := range logs {
		i.logs[l.Index] = l
		if i.lowIndex == 0 {
			i.lowIndex = l.Index
		}
		if l.Index > i.highIndex {
			i.highIndex = l.Index
		}
	}
	return nil
}

func (i *InmemStore) DeleteRange(min, max uint64) error {
	i.l.Lock()
	defer i.l.Unlock()
	for j := min; j <= max; j++ {
		delete(i.logs, j)
	}
	if min <= i.lowIndex {
		i.lowIndex = max + 1
	}
	if max >= i.highIndex {
		i.highIndex = min - 1
	}
	if i.lowIndex > i.highIndex {
		i.lowIndex = 0
		i.highIndex = 0
	}
	return nil
}

func (i *InmemStore) Set(key []byte, val []byte) error {
	i.l.Lock()
	defer i.l.Unlock()
	i.kv[string(key)] = val
	return nil
}

func (i *InmemStore) Get(key []byte) ([]byte, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.kv[string(key)], nil
}

func (i *InmemStore) SetUint64(key []byte, val uint64) error {
	i.l.Lock()
	defer i.l.Unlock()
	i.kvInt[string(key)] = val
	return nil
}

func (i *InmemStore) GetUint64(key []byte) (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.kvInt[string(key)], nil
}
