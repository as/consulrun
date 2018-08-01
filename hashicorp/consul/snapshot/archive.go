// The archive utilities manage the internal format of a snapshot, which is a
//
//
package snapshot

import (
	"archive/tar"
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/as/consulrun/hashicorp/raft"
)

type hashList struct {
	hashes map[string]hash.Hash
}

func newHashList() *hashList {
	return &hashList{
		hashes: make(map[string]hash.Hash),
	}
}

func (hl *hashList) Add(file string) hash.Hash {
	if existing, ok := hl.hashes[file]; ok {
		return existing
	}

	h := sha256.New()
	hl.hashes[file] = h
	return h
}

func (hl *hashList) Encode(w io.Writer) error {
	for file, h := range hl.hashes {
		if _, err := fmt.Fprintf(w, "%x  %s\n", h.Sum([]byte{}), file); err != nil {
			return err
		}
	}
	return nil
}

func (hl *hashList) DecodeAndVerify(r io.Reader) error {

	seen := make(map[string]struct{})
	s := bufio.NewScanner(r)
	for s.Scan() {
		sha := make([]byte, sha256.Size)
		var file string
		if _, err := fmt.Sscanf(s.Text(), "%x  %s", &sha, &file); err != nil {
			return err
		}

		h, ok := hl.hashes[file]
		if !ok {
			return fmt.Errorf("list missing hash for %q", file)
		}
		if !bytes.Equal(sha, h.Sum([]byte{})) {
			return fmt.Errorf("hash check failed for %q", file)
		}
		seen[file] = struct{}{}
	}
	if err := s.Err(); err != nil {
		return err
	}

	for file := range hl.hashes {
		if _, ok := seen[file]; !ok {
			return fmt.Errorf("file missing for %q", file)
		}
	}

	return nil
}

func write(out io.Writer, metadata *raft.SnapshotMeta, snap io.Reader) error {

	now := time.Now()
	archive := tar.NewWriter(out)

	hl := newHashList()

	metaHash := hl.Add("meta.json")
	var metaBuffer bytes.Buffer
	enc := json.NewEncoder(&metaBuffer)
	if err := enc.Encode(metadata); err != nil {
		return fmt.Errorf("failed to encode snapshot metadata: %v", err)
	}
	if err := archive.WriteHeader(&tar.Header{
		Name:    "meta.json",
		Mode:    0600,
		Size:    int64(metaBuffer.Len()),
		ModTime: now,
	}); err != nil {
		return fmt.Errorf("failed to write snapshot metadata header: %v", err)
	}
	if _, err := io.Copy(archive, io.TeeReader(&metaBuffer, metaHash)); err != nil {
		return fmt.Errorf("failed to write snapshot metadata: %v", err)
	}

	snapHash := hl.Add("state.bin")
	if err := archive.WriteHeader(&tar.Header{
		Name:    "state.bin",
		Mode:    0600,
		Size:    metadata.Size,
		ModTime: now,
	}); err != nil {
		return fmt.Errorf("failed to write snapshot data header: %v", err)
	}
	if _, err := io.CopyN(archive, io.TeeReader(snap, snapHash), metadata.Size); err != nil {
		return fmt.Errorf("failed to write snapshot metadata: %v", err)
	}

	var shaBuffer bytes.Buffer
	if err := hl.Encode(&shaBuffer); err != nil {
		return fmt.Errorf("failed to encode snapshot hashes: %v", err)
	}
	if err := archive.WriteHeader(&tar.Header{
		Name:    "SHA256SUMS",
		Mode:    0600,
		Size:    int64(shaBuffer.Len()),
		ModTime: now,
	}); err != nil {
		return fmt.Errorf("failed to write snapshot hashes header: %v", err)
	}
	if _, err := io.Copy(archive, &shaBuffer); err != nil {
		return fmt.Errorf("failed to write snapshot metadata: %v", err)
	}

	if err := archive.Close(); err != nil {
		return fmt.Errorf("failed to finalize snapshot: %v", err)
	}

	return nil
}

func read(in io.Reader, metadata *raft.SnapshotMeta, snap io.Writer) error {

	archive := tar.NewReader(in)

	hl := newHashList()

	metaHash := hl.Add("meta.json")
	snapHash := hl.Add("state.bin")

	var shaBuffer bytes.Buffer
	for {
		hdr, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed reading snapshot: %v", err)
		}

		switch hdr.Name {
		case "meta.json":
			dec := json.NewDecoder(io.TeeReader(archive, metaHash))
			if err := dec.Decode(&metadata); err != nil {
				return fmt.Errorf("failed to decode snapshot metadata: %v", err)
			}

		case "state.bin":
			if _, err := io.Copy(io.MultiWriter(snap, snapHash), archive); err != nil {
				return fmt.Errorf("failed to read or write snapshot data: %v", err)
			}

		case "SHA256SUMS":
			if _, err := io.Copy(&shaBuffer, archive); err != nil {
				return fmt.Errorf("failed to read snapshot hashes: %v", err)
			}

		default:
			return fmt.Errorf("unexpected file %q in snapshot", hdr.Name)
		}

	}

	if err := hl.DecodeAndVerify(&shaBuffer); err != nil {
		return fmt.Errorf("failed checking integrity of snapshot: %v", err)
	}

	return nil
}
