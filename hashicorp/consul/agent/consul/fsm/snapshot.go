package fsm

import (
	"fmt"
	"time"

	"github.com/armon/go-metrics"
	"github.com/as/consulrun/hashicorp/consul/agent/consul/state"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/go-msgpack/codec"
	"github.com/as/consulrun/hashicorp/raft"
)

type snapshot struct {
	state *state.Snapshot
}

type snapshotHeader struct {
	LastIndex uint64
}

type persister func(s *snapshot, sink raft.SnapshotSink, encoder *codec.Encoder) error

var persisters []persister

func registerPersister(fn persister) {
	persisters = append(persisters, fn)
}

type restorer func(header *snapshotHeader, restore *state.Restore, decoder *codec.Decoder) error

var restorers map[structs.MessageType]restorer

func registerRestorer(msg structs.MessageType, fn restorer) {
	if restorers == nil {
		restorers = make(map[structs.MessageType]restorer)
	}
	if restorers[msg] != nil {
		panic(fmt.Errorf("Message %d is already registered", msg))
	}
	restorers[msg] = fn
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	defer metrics.MeasureSince([]string{"consul", "fsm", "persist"}, time.Now())
	defer metrics.MeasureSince([]string{"fsm", "persist"}, time.Now())

	header := snapshotHeader{
		LastIndex: s.state.LastIndex(),
	}
	encoder := codec.NewEncoder(sink, msgpackHandle)
	if err := encoder.Encode(&header); err != nil {
		sink.Cancel()
		return err
	}

	for _, fn := range persisters {
		if err := fn(s, sink, encoder); err != nil {
			sink.Cancel()
			return err
		}
	}
	return nil
}

func (s *snapshot) Release() {
	s.state.Close()
}
