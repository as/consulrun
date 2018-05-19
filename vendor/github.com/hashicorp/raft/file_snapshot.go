package raft

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

const (
	testPath      = "permTest"
	snapPath      = "snapshots"
	metaFilePath  = "meta.json"
	stateFilePath = "state.bin"
	tmpSuffix     = ".tmp"
)

type FileSnapshotStore struct {
	path   string
	retain int
	logger *log.Logger
}

type snapMetaSlice []*fileSnapshotMeta

type FileSnapshotSink struct {
	store     *FileSnapshotStore
	logger    *log.Logger
	dir       string
	parentDir string
	meta      fileSnapshotMeta

	stateFile *os.File
	stateHash hash.Hash64
	buffered  *bufio.Writer

	closed bool
}

type fileSnapshotMeta struct {
	SnapshotMeta
	CRC []byte
}

type bufferedFile struct {
	bh *bufio.Reader
	fh *os.File
}

func (b *bufferedFile) Read(p []byte) (n int, err error) {
	return b.bh.Read(p)
}

func (b *bufferedFile) Close() error {
	return b.fh.Close()
}

func NewFileSnapshotStoreWithLogger(base string, retain int, logger *log.Logger) (*FileSnapshotStore, error) {
	if retain < 1 {
		return nil, fmt.Errorf("must retain at least one snapshot")
	}
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	path := filepath.Join(base, snapPath)
	if err := os.MkdirAll(path, 0755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("snapshot path not accessible: %v", err)
	}

	store := &FileSnapshotStore{
		path:   path,
		retain: retain,
		logger: logger,
	}

	if err := store.testPermissions(); err != nil {
		return nil, fmt.Errorf("permissions test failed: %v", err)
	}
	return store, nil
}

func NewFileSnapshotStore(base string, retain int, logOutput io.Writer) (*FileSnapshotStore, error) {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	return NewFileSnapshotStoreWithLogger(base, retain, log.New(logOutput, "", log.LstdFlags))
}

func (f *FileSnapshotStore) testPermissions() error {
	path := filepath.Join(f.path, testPath)
	fh, err := os.Create(path)
	if err != nil {
		return err
	}

	if err = fh.Close(); err != nil {
		return err
	}

	if err = os.Remove(path); err != nil {
		return err
	}
	return nil
}

func snapshotName(term, index uint64) string {
	now := time.Now()
	msec := now.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
}

func (f *FileSnapshotStore) Create(version SnapshotVersion, index, term uint64,
	configuration Configuration, configurationIndex uint64, trans Transport) (SnapshotSink, error) {

	if version != 1 {
		return nil, fmt.Errorf("unsupported snapshot version %d", version)
	}

	name := snapshotName(term, index)
	path := filepath.Join(f.path, name+tmpSuffix)
	f.logger.Printf("[INFO] snapshot: Creating new snapshot at %s", path)

	if err := os.MkdirAll(path, 0755); err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to make snapshot directory: %v", err)
		return nil, err
	}

	sink := &FileSnapshotSink{
		store:     f,
		logger:    f.logger,
		dir:       path,
		parentDir: f.path,
		meta: fileSnapshotMeta{
			SnapshotMeta: SnapshotMeta{
				Version:            version,
				ID:                 name,
				Index:              index,
				Term:               term,
				Peers:              encodePeers(configuration, trans),
				Configuration:      configuration,
				ConfigurationIndex: configurationIndex,
			},
			CRC: nil,
		},
	}

	if err := sink.writeMeta(); err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to write metadata: %v", err)
		return nil, err
	}

	statePath := filepath.Join(path, stateFilePath)
	fh, err := os.Create(statePath)
	if err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to create state file: %v", err)
		return nil, err
	}
	sink.stateFile = fh

	sink.stateHash = crc64.New(crc64.MakeTable(crc64.ECMA))

	multi := io.MultiWriter(sink.stateFile, sink.stateHash)
	sink.buffered = bufio.NewWriter(multi)

	return sink, nil
}

func (f *FileSnapshotStore) List() ([]*SnapshotMeta, error) {

	snapshots, err := f.getSnapshots()
	if err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to get snapshots: %v", err)
		return nil, err
	}

	var snapMeta []*SnapshotMeta
	for _, meta := range snapshots {
		snapMeta = append(snapMeta, &meta.SnapshotMeta)
		if len(snapMeta) == f.retain {
			break
		}
	}
	return snapMeta, nil
}

func (f *FileSnapshotStore) getSnapshots() ([]*fileSnapshotMeta, error) {

	snapshots, err := ioutil.ReadDir(f.path)
	if err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to scan snapshot dir: %v", err)
		return nil, err
	}

	var snapMeta []*fileSnapshotMeta
	for _, snap := range snapshots {

		if !snap.IsDir() {
			continue
		}

		dirName := snap.Name()
		if strings.HasSuffix(dirName, tmpSuffix) {
			f.logger.Printf("[WARN] snapshot: Found temporary snapshot: %v", dirName)
			continue
		}

		meta, err := f.readMeta(dirName)
		if err != nil {
			f.logger.Printf("[WARN] snapshot: Failed to read metadata for %v: %v", dirName, err)
			continue
		}

		if meta.Version < SnapshotVersionMin || meta.Version > SnapshotVersionMax {
			f.logger.Printf("[WARN] snapshot: Snapshot version for %v not supported: %d", dirName, meta.Version)
			continue
		}

		snapMeta = append(snapMeta, meta)
	}

	sort.Sort(sort.Reverse(snapMetaSlice(snapMeta)))

	return snapMeta, nil
}

func (f *FileSnapshotStore) readMeta(name string) (*fileSnapshotMeta, error) {

	metaPath := filepath.Join(f.path, name, metaFilePath)
	fh, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	buffered := bufio.NewReader(fh)

	meta := &fileSnapshotMeta{}
	dec := json.NewDecoder(buffered)
	if err := dec.Decode(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (f *FileSnapshotStore) Open(id string) (*SnapshotMeta, io.ReadCloser, error) {

	meta, err := f.readMeta(id)
	if err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to get meta data to open snapshot: %v", err)
		return nil, nil, err
	}

	statePath := filepath.Join(f.path, id, stateFilePath)
	fh, err := os.Open(statePath)
	if err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to open state file: %v", err)
		return nil, nil, err
	}

	stateHash := crc64.New(crc64.MakeTable(crc64.ECMA))

	_, err = io.Copy(stateHash, fh)
	if err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to read state file: %v", err)
		fh.Close()
		return nil, nil, err
	}

	computed := stateHash.Sum(nil)
	if bytes.Compare(meta.CRC, computed) != 0 {
		f.logger.Printf("[ERR] snapshot: CRC checksum failed (stored: %v computed: %v)",
			meta.CRC, computed)
		fh.Close()
		return nil, nil, fmt.Errorf("CRC mismatch")
	}

	if _, err := fh.Seek(0, 0); err != nil {
		f.logger.Printf("[ERR] snapshot: State file seek failed: %v", err)
		fh.Close()
		return nil, nil, err
	}

	buffered := &bufferedFile{
		bh: bufio.NewReader(fh),
		fh: fh,
	}

	return &meta.SnapshotMeta, buffered, nil
}

func (f *FileSnapshotStore) ReapSnapshots() error {
	snapshots, err := f.getSnapshots()
	if err != nil {
		f.logger.Printf("[ERR] snapshot: Failed to get snapshots: %v", err)
		return err
	}

	for i := f.retain; i < len(snapshots); i++ {
		path := filepath.Join(f.path, snapshots[i].ID)
		f.logger.Printf("[INFO] snapshot: reaping snapshot %v", path)
		if err := os.RemoveAll(path); err != nil {
			f.logger.Printf("[ERR] snapshot: Failed to reap snapshot %v: %v", path, err)
			return err
		}
	}
	return nil
}

func (s *FileSnapshotSink) ID() string {
	return s.meta.ID
}

func (s *FileSnapshotSink) Write(b []byte) (int, error) {
	return s.buffered.Write(b)
}

func (s *FileSnapshotSink) Close() error {

	if s.closed {
		return nil
	}
	s.closed = true

	if err := s.finalize(); err != nil {
		s.logger.Printf("[ERR] snapshot: Failed to finalize snapshot: %v", err)
		if delErr := os.RemoveAll(s.dir); delErr != nil {
			s.logger.Printf("[ERR] snapshot: Failed to delete temporary snapshot directory at path %v: %v", s.dir, delErr)
			return delErr
		}
		return err
	}

	if err := s.writeMeta(); err != nil {
		s.logger.Printf("[ERR] snapshot: Failed to write metadata: %v", err)
		return err
	}

	newPath := strings.TrimSuffix(s.dir, tmpSuffix)
	if err := os.Rename(s.dir, newPath); err != nil {
		s.logger.Printf("[ERR] snapshot: Failed to move snapshot into place: %v", err)
		return err
	}

	if runtime.GOOS != "windows" { //skipping fsync for directory entry edits on Windows, only needed for *nix style file systems
		parentFH, err := os.Open(s.parentDir)
		defer parentFH.Close()
		if err != nil {
			s.logger.Printf("[ERR] snapshot: Failed to open snapshot parent directory %v, error: %v", s.parentDir, err)
			return err
		}

		if err = parentFH.Sync(); err != nil {
			s.logger.Printf("[ERR] snapshot: Failed syncing parent directory %v, error: %v", s.parentDir, err)
			return err
		}
	}

	if err := s.store.ReapSnapshots(); err != nil {
		return err
	}

	return nil
}

func (s *FileSnapshotSink) Cancel() error {

	if s.closed {
		return nil
	}
	s.closed = true

	if err := s.finalize(); err != nil {
		s.logger.Printf("[ERR] snapshot: Failed to finalize snapshot: %v", err)
		return err
	}

	return os.RemoveAll(s.dir)
}

func (s *FileSnapshotSink) finalize() error {

	if err := s.buffered.Flush(); err != nil {
		return err
	}

	if err := s.stateFile.Sync(); err != nil {
		return err
	}

	stat, statErr := s.stateFile.Stat()

	if err := s.stateFile.Close(); err != nil {
		return err
	}

	if statErr != nil {
		return statErr
	}
	s.meta.Size = stat.Size()

	s.meta.CRC = s.stateHash.Sum(nil)
	return nil
}

func (s *FileSnapshotSink) writeMeta() error {

	metaPath := filepath.Join(s.dir, metaFilePath)
	fh, err := os.Create(metaPath)
	if err != nil {
		return err
	}
	defer fh.Close()

	buffered := bufio.NewWriter(fh)

	enc := json.NewEncoder(buffered)
	if err := enc.Encode(&s.meta); err != nil {
		return err
	}

	if err = buffered.Flush(); err != nil {
		return err
	}

	if err = fh.Sync(); err != nil {
		return err
	}

	return nil
}

func (s snapMetaSlice) Len() int {
	return len(s)
}

func (s snapMetaSlice) Less(i, j int) bool {
	if s[i].Term != s[j].Term {
		return s[i].Term < s[j].Term
	}
	if s[i].Index != s[j].Index {
		return s[i].Index < s[j].Index
	}
	return s[i].ID < s[j].ID
}

func (s snapMetaSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
