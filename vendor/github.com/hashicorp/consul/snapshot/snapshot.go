// snapshot manages the interactions between Consul and Raft in order to take
package snapshot

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/hashicorp/raft"
)

type Snapshot struct {
	file  *os.File
	index uint64
}

func New(logger *log.Logger, r *raft.Raft) (*Snapshot, error) {

	future := r.Snapshot()
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("Raft error when taking snapshot: %v", err)
	}

	metadata, snap, err := future.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshot: %v:", err)
	}
	defer func() {
		if err := snap.Close(); err != nil {
			logger.Printf("[ERR] snapshot: Failed to close Raft snapshot: %v", err)
		}
	}()

	archive, err := ioutil.TempFile("", "snapshot")
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot file: %v", err)
	}

	var keep bool
	defer func() {
		if keep {
			return
		}

		if err := os.Remove(archive.Name()); err != nil {
			logger.Printf("[ERR] snapshot: Failed to clean up temp snapshot: %v", err)
		}
	}()

	compressor := gzip.NewWriter(archive)

	if err := write(compressor, metadata, snap); err != nil {
		return nil, fmt.Errorf("failed to write snapshot file: %v", err)
	}

	if err := compressor.Close(); err != nil {
		return nil, fmt.Errorf("failed to compress snapshot file: %v", err)
	}

	if err := archive.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync snapshot: %v", err)
	}
	if _, err := archive.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to rewind snapshot: %v", err)
	}

	keep = true
	return &Snapshot{archive, metadata.Index}, nil
}

func (s *Snapshot) Index() uint64 {
	if s == nil {
		return 0
	}
	return s.index
}

func (s *Snapshot) Read(p []byte) (n int, err error) {
	if s == nil {
		return 0, io.EOF
	}
	return s.file.Read(p)
}

func (s *Snapshot) Close() error {
	if s == nil {
		return nil
	}

	if err := s.file.Close(); err != nil {
		return err
	}
	return os.Remove(s.file.Name())
}

func Verify(in io.Reader) (*raft.SnapshotMeta, error) {

	decomp, err := gzip.NewReader(in)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress snapshot: %v", err)
	}
	defer decomp.Close()

	var metadata raft.SnapshotMeta
	if err := read(decomp, &metadata, ioutil.Discard); err != nil {
		return nil, fmt.Errorf("failed to read snapshot file: %v", err)
	}
	return &metadata, nil
}

func Restore(logger *log.Logger, in io.Reader, r *raft.Raft) error {

	decomp, err := gzip.NewReader(in)
	if err != nil {
		return fmt.Errorf("failed to decompress snapshot: %v", err)
	}
	defer func() {
		if err := decomp.Close(); err != nil {
			logger.Printf("[ERR] snapshot: Failed to close snapshot decompressor: %v", err)
		}
	}()

	snap, err := ioutil.TempFile("", "snapshot")
	if err != nil {
		return fmt.Errorf("failed to create temp snapshot file: %v", err)
	}
	defer func() {
		if err := snap.Close(); err != nil {
			logger.Printf("[ERR] snapshot: Failed to close temp snapshot: %v", err)
		}
		if err := os.Remove(snap.Name()); err != nil {
			logger.Printf("[ERR] snapshot: Failed to clean up temp snapshot: %v", err)
		}
	}()

	var metadata raft.SnapshotMeta
	if err := read(decomp, &metadata, snap); err != nil {
		return fmt.Errorf("failed to read snapshot file: %v", err)
	}

	if err := snap.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp snapshot: %v", err)
	}
	if _, err := snap.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to rewind temp snapshot: %v", err)
	}

	if err := r.Restore(&metadata, snap, 0); err != nil {
		return fmt.Errorf("Raft error when restoring snapshot: %v", err)
	}

	return nil
}
