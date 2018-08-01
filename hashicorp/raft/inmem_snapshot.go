package raft

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
)

type InmemSnapshotStore struct {
	latest      *InmemSnapshotSink
	hasSnapshot bool
	sync.RWMutex
}

type InmemSnapshotSink struct {
	meta     SnapshotMeta
	contents *bytes.Buffer
}

func NewInmemSnapshotStore() *InmemSnapshotStore {
	return &InmemSnapshotStore{
		latest: &InmemSnapshotSink{
			contents: &bytes.Buffer{},
		},
	}
}

func (m *InmemSnapshotStore) Create(version SnapshotVersion, index, term uint64,
	configuration Configuration, configurationIndex uint64, trans Transport) (SnapshotSink, error) {

	if version != 1 {
		return nil, fmt.Errorf("unsupported snapshot version %d", version)
	}

	name := snapshotName(term, index)

	m.Lock()
	defer m.Unlock()

	sink := &InmemSnapshotSink{
		meta: SnapshotMeta{
			Version:            version,
			ID:                 name,
			Index:              index,
			Term:               term,
			Peers:              encodePeers(configuration, trans),
			Configuration:      configuration,
			ConfigurationIndex: configurationIndex,
		},
		contents: &bytes.Buffer{},
	}
	m.hasSnapshot = true
	m.latest = sink

	return sink, nil
}

func (m *InmemSnapshotStore) List() ([]*SnapshotMeta, error) {
	m.RLock()
	defer m.RUnlock()

	if !m.hasSnapshot {
		return []*SnapshotMeta{}, nil
	}
	return []*SnapshotMeta{&m.latest.meta}, nil
}

func (m *InmemSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	m.RLock()
	defer m.RUnlock()

	if m.latest.meta.ID != id {
		return nil, nil, fmt.Errorf("[ERR] snapshot: failed to open snapshot id: %s", id)
	}

	return &m.latest.meta, ioutil.NopCloser(m.latest.contents), nil
}

func (s *InmemSnapshotSink) Write(p []byte) (n int, err error) {
	written, err := io.Copy(s.contents, bytes.NewReader(p))
	s.meta.Size += written
	return int(written), err
}

func (s *InmemSnapshotSink) Close() error {
	return nil
}

func (s *InmemSnapshotSink) ID() string {
	return s.meta.ID
}

func (s *InmemSnapshotSink) Cancel() error {
	return nil
}
