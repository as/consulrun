package api

import (
	"bytes"
	"strings"
	"testing"
)

func TestAPI_Snapshot(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	kv := c.KV()
	key := &KVPair{Key: testKey(), Value: []byte("hello")}
	if _, err := kv.Put(key, nil); err != nil {
		t.Fatalf("err: %v", err)
	}

	pair, _, err := kv.Get(key.Key, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if pair == nil {
		t.Fatalf("expected value: %#v", pair)
	}
	if !bytes.Equal(pair.Value, []byte("hello")) {
		t.Fatalf("unexpected value: %#v", pair)
	}

	snapshot := c.Snapshot()
	snap, qm, err := snapshot.Save(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer snap.Close()

	if qm.LastIndex == 0 || !qm.KnownLeader ||
		qm.RequestTime == 0 {
		t.Fatalf("bad: %v", qm)
	}

	key.Value = []byte("goodbye")
	if _, err := kv.Put(key, nil); err != nil {
		t.Fatalf("err: %v", err)
	}

	pair, _, err = kv.Get(key.Key, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if pair == nil {
		t.Fatalf("expected value: %#v", pair)
	}
	if !bytes.Equal(pair.Value, []byte("goodbye")) {
		t.Fatalf("unexpected value: %#v", pair)
	}

	if err := snapshot.Restore(nil, snap); err != nil {
		t.Fatalf("err: %v", err)
	}

	pair, _, err = kv.Get(key.Key, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if pair == nil {
		t.Fatalf("expected value: %#v", pair)
	}
	if !bytes.Equal(pair.Value, []byte("hello")) {
		t.Fatalf("unexpected value: %#v", pair)
	}
}

func TestAPI_Snapshot_Options(t *testing.T) {
	t.Parallel()
	c, s := makeACLClient(t)
	defer s.Stop()

	snapshot := c.Snapshot()
	_, _, err := snapshot.Save(&QueryOptions{Token: "anonymous"})
	if err == nil || !strings.Contains(err.Error(), "Permission denied") {
		t.Fatalf("err: %v", err)
	}

	_, _, err = snapshot.Save(&QueryOptions{Datacenter: "nope"})
	if err == nil || !strings.Contains(err.Error(), "No path to datacenter") {
		t.Fatalf("err: %v", err)
	}

	snap, _, err := snapshot.Save(&QueryOptions{Token: "root"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer snap.Close()

	snap, _, err = snapshot.Save(&QueryOptions{Token: "root", AllowStale: true})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer snap.Close()

	null := bytes.NewReader([]byte(""))
	err = snapshot.Restore(&WriteOptions{Token: "anonymous"}, null)
	if err == nil || !strings.Contains(err.Error(), "Permission denied") {
		t.Fatalf("err: %v", err)
	}

	null = bytes.NewReader([]byte(""))
	err = snapshot.Restore(&WriteOptions{Datacenter: "nope"}, null)
	if err == nil || !strings.Contains(err.Error(), "No path to datacenter") {
		t.Fatalf("err: %v", err)
	}

	if err := snapshot.Restore(&WriteOptions{Token: "root"}, snap); err != nil {
		t.Fatalf("err: %v", err)
	}
}
