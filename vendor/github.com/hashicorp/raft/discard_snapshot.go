package raft

import (
	"fmt"
	"io"
)

type DiscardSnapshotStore struct{}

type DiscardSnapshotSink struct{}

func NewDiscardSnapshotStore() *DiscardSnapshotStore {
	return &DiscardSnapshotStore{}
}

func (d *DiscardSnapshotStore) Create(version SnapshotVersion, index, term uint64,
	configuration Configuration, configurationIndex uint64, trans Transport) (SnapshotSink, error) {
	return &DiscardSnapshotSink{}, nil
}

func (d *DiscardSnapshotStore) List() ([]*SnapshotMeta, error) {
	return nil, nil
}

func (d *DiscardSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {
	return nil, nil, fmt.Errorf("open is not supported")
}

func (d *DiscardSnapshotSink) Write(b []byte) (int, error) {
	return len(b), nil
}

func (d *DiscardSnapshotSink) Close() error {
	return nil
}

func (d *DiscardSnapshotSink) ID() string {
	return "discard"
}

func (d *DiscardSnapshotSink) Cancel() error {
	return nil
}
