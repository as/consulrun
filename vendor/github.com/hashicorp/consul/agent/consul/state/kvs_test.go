package state

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/go-memdb"
)

func TestStateStore_GC(t *testing.T) {

	ttl := 10 * time.Millisecond
	gran := 5 * time.Millisecond
	gc, err := NewTombstoneGC(ttl, gran)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	gc.SetEnabled(true)
	s, err := NewStateStore(gc)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	testSetKey(t, s, 1, "foo", "foo")
	testSetKey(t, s, 2, "foo/bar", "bar")
	testSetKey(t, s, 3, "foo/baz", "bar")
	testSetKey(t, s, 4, "foo/moo", "bar")
	testSetKey(t, s, 5, "foo/zoo", "bar")

	if err := s.KVSDelete(6, "foo/zoo"); err != nil {
		t.Fatalf("err: %s", err)
	}
	select {
	case idx := <-gc.ExpireCh():
		if idx != 6 {
			t.Fatalf("bad index: %d", idx)
		}
	case <-time.After(2 * ttl):
		t.Fatalf("GC never fired")
	}

	if err := s.KVSDeleteTree(7, "foo/moo"); err != nil {
		t.Fatalf("err: %s", err)
	}
	select {
	case idx := <-gc.ExpireCh():
		if idx != 7 {
			t.Fatalf("bad index: %d", idx)
		}
	case <-time.After(2 * ttl):
		t.Fatalf("GC never fired")
	}

	if ok, err := s.KVSDeleteCAS(8, 3, "foo/baz"); !ok || err != nil {
		t.Fatalf("err: %s", err)
	}
	select {
	case idx := <-gc.ExpireCh():
		if idx != 8 {
			t.Fatalf("bad index: %d", idx)
		}
	case <-time.After(2 * ttl):
		t.Fatalf("GC never fired")
	}

	testRegisterNode(t, s, 9, "node1")
	session := &structs.Session{
		ID:       testUUID(),
		Node:     "node1",
		Behavior: structs.SessionKeysDelete,
	}
	if err := s.SessionCreate(10, session); err != nil {
		t.Fatalf("err: %s", err)
	}
	d := &structs.DirEntry{
		Key:     "lock",
		Session: session.ID,
	}
	if ok, err := s.KVSLock(11, d); !ok || err != nil {
		t.Fatalf("err: %v", err)
	}
	if err := s.SessionDestroy(12, session.ID); err != nil {
		t.Fatalf("err: %s", err)
	}
	select {
	case idx := <-gc.ExpireCh():
		if idx != 12 {
			t.Fatalf("bad index: %d", idx)
		}
	case <-time.After(2 * ttl):
		t.Fatalf("GC never fired")
	}
}

func TestStateStore_ReapTombstones(t *testing.T) {
	s := testStateStore(t)

	testSetKey(t, s, 1, "foo", "foo")
	testSetKey(t, s, 2, "foo/bar", "bar")
	testSetKey(t, s, 3, "foo/baz", "bar")
	testSetKey(t, s, 4, "foo/moo", "bar")
	testSetKey(t, s, 5, "foo/zoo", "bar")

	if err := s.KVSDelete(6, "foo/baz"); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := s.KVSDelete(7, "foo/moo"); err != nil {
		t.Fatalf("err: %s", err)
	}

	idx, _, err := s.KVSList(nil, "foo/")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 7 {
		t.Fatalf("bad index: %d", idx)
	}

	if err := s.ReapTombstones(6); err != nil {
		t.Fatalf("err: %s", err)
	}

	idx, _, err = s.KVSList(nil, "foo/")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 7 {
		t.Fatalf("bad index: %d", idx)
	}

	if err := s.ReapTombstones(7); err != nil {
		t.Fatalf("err: %s", err)
	}

	idx, _, err = s.KVSList(nil, "foo/")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 5 {
		t.Fatalf("bad index: %d", idx)
	}

	snap := s.Snapshot()
	defer snap.Close()
	stones, err := snap.Tombstones()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if stones.Next() != nil {
		t.Fatalf("unexpected extra tombstones")
	}
}

func TestStateStore_KVSSet_KVSGet(t *testing.T) {
	s := testStateStore(t)

	ws := memdb.NewWatchSet()
	idx, result, err := s.KVSGet(ws, "foo")
	if result != nil || err != nil || idx != 0 {
		t.Fatalf("expected (0, nil, nil), got : (%#v, %#v, %#v)", idx, result, err)
	}

	entry := &structs.DirEntry{
		Key:   "foo",
		Value: []byte("bar"),
	}
	if err := s.KVSSet(1, entry); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}

	ws = memdb.NewWatchSet()
	idx, result, err = s.KVSGet(ws, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result == nil {
		t.Fatalf("expected k/v pair, got nothing")
	}
	if idx != 1 {
		t.Fatalf("bad index: %d", idx)
	}

	if result.CreateIndex != 1 || result.ModifyIndex != 1 {
		t.Fatalf("bad index: %d, %d", result.CreateIndex, result.ModifyIndex)
	}

	if v := string(result.Value); v != "bar" {
		t.Fatalf("expected 'bar', got: '%s'", v)
	}

	update := &structs.DirEntry{
		Key:   "foo",
		Value: []byte("baz"),
	}
	if err := s.KVSSet(2, update); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}

	ws = memdb.NewWatchSet()
	idx, result, err = s.KVSGet(ws, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result.CreateIndex != 1 || result.ModifyIndex != 2 {
		t.Fatalf("bad index: %d, %d", result.CreateIndex, result.ModifyIndex)
	}
	if v := string(result.Value); v != "baz" {
		t.Fatalf("expected 'baz', got '%s'", v)
	}
	if idx != 2 {
		t.Fatalf("bad index: %d", idx)
	}

	update = &structs.DirEntry{
		Key:     "foo",
		Value:   []byte("zoo"),
		Session: "nope",
	}
	if err := s.KVSSet(3, update); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}

	ws = memdb.NewWatchSet()
	idx, result, err = s.KVSGet(ws, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result.CreateIndex != 1 || result.ModifyIndex != 3 {
		t.Fatalf("bad index: %d, %d", result.CreateIndex, result.ModifyIndex)
	}
	if v := string(result.Value); v != "zoo" {
		t.Fatalf("expected 'zoo', got '%s'", v)
	}
	if result.Session != "" {
		t.Fatalf("expected empty session, got '%s", result.Session)
	}
	if idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}

	testRegisterNode(t, s, 4, "node1")
	session := testUUID()
	if err := s.SessionCreate(5, &structs.Session{ID: session, Node: "node1"}); err != nil {
		t.Fatalf("err: %s", err)
	}
	update = &structs.DirEntry{
		Key:     "foo",
		Value:   []byte("locked"),
		Session: session,
	}
	ok, err := s.KVSLock(6, update)
	if !ok || err != nil {
		t.Fatalf("didn't get the lock: %v %s", ok, err)
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}

	ws = memdb.NewWatchSet()
	idx, result, err = s.KVSGet(ws, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result.CreateIndex != 1 || result.ModifyIndex != 6 {
		t.Fatalf("bad index: %d, %d", result.CreateIndex, result.ModifyIndex)
	}
	if v := string(result.Value); v != "locked" {
		t.Fatalf("expected 'zoo', got '%s'", v)
	}
	if result.Session != session {
		t.Fatalf("expected session, got '%s", result.Session)
	}
	if idx != 6 {
		t.Fatalf("bad index: %d", idx)
	}

	update = &structs.DirEntry{
		Key:   "foo",
		Value: []byte("stoleit"),
	}
	if err := s.KVSSet(7, update); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}

	ws = memdb.NewWatchSet()
	idx, result, err = s.KVSGet(ws, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result.CreateIndex != 1 || result.ModifyIndex != 7 {
		t.Fatalf("bad index: %d, %d", result.CreateIndex, result.ModifyIndex)
	}
	if v := string(result.Value); v != "stoleit" {
		t.Fatalf("expected 'zoo', got '%s'", v)
	}
	if result.Session != session {
		t.Fatalf("expected session, got '%s", result.Session)
	}
	if idx != 7 {
		t.Fatalf("bad index: %d", idx)
	}

	testSetKey(t, s, 8, "other", "yup")
	if watchFired(ws) {
		t.Fatalf("bad")
	}

	idx, result, err = s.KVSGet(nil, "nope")
	if result != nil || err != nil || idx != 8 {
		t.Fatalf("expected (8, nil, nil), got : (%#v, %#v, %#v)", idx, result, err)
	}
}

func TestStateStore_KVSList(t *testing.T) {
	s := testStateStore(t)

	ws := memdb.NewWatchSet()
	idx, entries, err := s.KVSList(ws, "")
	if idx != 0 || entries != nil || err != nil {
		t.Fatalf("expected (0, nil, nil), got: (%d, %#v, %#v)", idx, entries, err)
	}

	testSetKey(t, s, 1, "foo", "foo")
	testSetKey(t, s, 2, "foo/bar", "bar")
	testSetKey(t, s, 3, "foo/bar/zip", "zip")
	testSetKey(t, s, 4, "foo/bar/zip/zorp", "zorp")
	testSetKey(t, s, 5, "foo/bar/baz", "baz")
	if !watchFired(ws) {
		t.Fatalf("bad")
	}

	idx, entries, err = s.KVSList(nil, "")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 5 {
		t.Fatalf("bad index: %d", idx)
	}

	if n := len(entries); n != 5 {
		t.Fatalf("expected 5 kvs entries, got: %d", n)
	}

	idx, entries, err = s.KVSList(nil, "foo/bar/zip")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}

	if n := len(entries); n != 2 {
		t.Fatalf("expected 2 kvs entries, got: %d", n)
	}
	if entries[0].Key != "foo/bar/zip" || entries[1].Key != "foo/bar/zip/zorp" {
		t.Fatalf("bad: %#v", entries)
	}

	ws = memdb.NewWatchSet()
	idx, _, err = s.KVSList(ws, "foo/bar/baz")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := s.KVSDelete(6, "foo/bar/baz"); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}
	ws = memdb.NewWatchSet()
	idx, _, err = s.KVSList(ws, "foo/bar/baz")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 6 {
		t.Fatalf("bad index: %d", idx)
	}

	testSetKey(t, s, 7, "some/other/key", "")
	if watchFired(ws) {
		t.Fatalf("bad")
	}

	idx, _, err = s.KVSList(nil, "foo/bar/baz")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 6 {
		t.Fatalf("bad index: %d", idx)
	}

	if err := s.ReapTombstones(6); err != nil {
		t.Fatalf("err: %s", err)
	}
	idx, _, err = s.KVSList(nil, "foo/bar/baz")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 7 {
		t.Fatalf("bad index: %d", idx)
	}

	idx, _, err = s.KVSList(nil, "")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 7 {
		t.Fatalf("bad index: %d", idx)
	}
}

func TestStateStore_KVSListKeys(t *testing.T) {
	s := testStateStore(t)

	ws := memdb.NewWatchSet()
	idx, keys, err := s.KVSListKeys(ws, "", "")
	if idx != 0 || keys != nil || err != nil {
		t.Fatalf("expected (0, nil, nil), got: (%d, %#v, %#v)", idx, keys, err)
	}

	testSetKey(t, s, 1, "foo", "foo")
	testSetKey(t, s, 2, "foo/bar", "bar")
	testSetKey(t, s, 3, "foo/bar/baz", "baz")
	testSetKey(t, s, 4, "foo/bar/zip", "zip")
	testSetKey(t, s, 5, "foo/bar/zip/zam", "zam")
	testSetKey(t, s, 6, "foo/bar/zip/zorp", "zorp")
	testSetKey(t, s, 7, "some/other/prefix", "nack")
	if !watchFired(ws) {
		t.Fatalf("bad")
	}

	idx, keys, err = s.KVSListKeys(nil, "", "")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if len(keys) != 7 {
		t.Fatalf("bad keys: %#v", keys)
	}
	if idx != 7 {
		t.Fatalf("bad index: %d", idx)
	}

	idx, keys, err = s.KVSListKeys(nil, "foo/bar/", "/")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if len(keys) != 3 {
		t.Fatalf("bad keys: %#v", keys)
	}
	if idx != 6 {
		t.Fatalf("bad index: %d", idx)
	}

	expect := []string{"foo/bar/baz", "foo/bar/zip", "foo/bar/zip/"}
	if !reflect.DeepEqual(keys, expect) {
		t.Fatalf("bad keys: %#v", keys)
	}

	idx, keys, err = s.KVSListKeys(nil, "foo", "")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 6 {
		t.Fatalf("bad index: %d", idx)
	}
	expect = []string{"foo", "foo/bar", "foo/bar/baz", "foo/bar/zip",
		"foo/bar/zip/zam", "foo/bar/zip/zorp"}
	if !reflect.DeepEqual(keys, expect) {
		t.Fatalf("bad keys: %#v", keys)
	}

	ws = memdb.NewWatchSet()
	idx, _, err = s.KVSListKeys(ws, "foo/bar/baz", "")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := s.KVSDelete(8, "foo/bar/baz"); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}
	ws = memdb.NewWatchSet()
	idx, _, err = s.KVSListKeys(ws, "foo/bar/baz", "")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 8 {
		t.Fatalf("bad index: %d", idx)
	}

	testSetKey(t, s, 9, "some/other/key", "")
	if watchFired(ws) {
		t.Fatalf("bad")
	}

	idx, _, err = s.KVSListKeys(nil, "foo/bar/baz", "")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 8 {
		t.Fatalf("bad index: %d", idx)
	}

	if err := s.ReapTombstones(8); err != nil {
		t.Fatalf("err: %s", err)
	}
	idx, _, err = s.KVSListKeys(nil, "foo/bar/baz", "")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 9 {
		t.Fatalf("bad index: %d", idx)
	}

	idx, _, err = s.KVSListKeys(nil, "", "")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 9 {
		t.Fatalf("bad index: %d", idx)
	}
}

func TestStateStore_KVSDelete(t *testing.T) {
	s := testStateStore(t)

	testSetKey(t, s, 1, "foo", "foo")
	testSetKey(t, s, 2, "foo/bar", "bar")

	if err := s.KVSDelete(3, "foo"); err != nil {
		t.Fatalf("err: %s", err)
	}

	tx := s.db.Txn(false)
	defer tx.Abort()
	e, err := tx.First("kvs", "id", "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if e != nil {
		t.Fatalf("expected kvs entry to be deleted, got: %#v", e)
	}

	e, err = tx.First("kvs", "id", "foo/bar")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if e == nil || string(e.(*structs.DirEntry).Value) != "bar" {
		t.Fatalf("bad kvs entry: %#v", e)
	}

	if idx := s.maxIndex("kvs"); idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}

	idx, _, err := s.KVSList(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}

	if err := s.ReapTombstones(3); err != nil {
		t.Fatalf("err: %s", err)
	}
	idx, _, err = s.KVSList(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 2 {
		t.Fatalf("bad index: %d", idx)
	}

	if err := s.KVSDelete(4, "foo"); err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx := s.maxIndex("kvs"); idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}
}

func TestStateStore_KVSDeleteCAS(t *testing.T) {
	s := testStateStore(t)

	testSetKey(t, s, 1, "foo", "foo")
	testSetKey(t, s, 2, "bar", "bar")
	testSetKey(t, s, 3, "baz", "baz")

	ok, err := s.KVSDeleteCAS(4, 1, "bar")
	if ok || err != nil {
		t.Fatalf("expected (false, nil), got: (%v, %#v)", ok, err)
	}

	idx, e, err := s.KVSGet(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if e == nil {
		t.Fatalf("expected a kvs entry, got nil")
	}
	if idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}

	ok, err = s.KVSDeleteCAS(4, 2, "bar")
	if !ok || err != nil {
		t.Fatalf("expected (true, nil), got: (%v, %#v)", ok, err)
	}

	idx, e, err = s.KVSGet(nil, "bar")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if e != nil {
		t.Fatalf("entry should be deleted")
	}
	if idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}

	testSetKey(t, s, 5, "some/other/key", "baz")

	idx, _, err = s.KVSList(nil, "bar")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}

	if err := s.ReapTombstones(4); err != nil {
		t.Fatalf("err: %s", err)
	}
	idx, _, err = s.KVSList(nil, "bar")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 5 {
		t.Fatalf("bad index: %d", idx)
	}

	ok, err = s.KVSDeleteCAS(6, 2, "bar")
	if !ok || err != nil {
		t.Fatalf("expected (true, nil), got: (%v, %#v)", ok, err)
	}
	if idx := s.maxIndex("kvs"); idx != 5 {
		t.Fatalf("bad index: %d", idx)
	}
}

func TestStateStore_KVSSetCAS(t *testing.T) {
	s := testStateStore(t)

	entry := &structs.DirEntry{
		Key:   "foo",
		Value: []byte("foo"),
		RaftIndex: structs.RaftIndex{
			CreateIndex: 1,
			ModifyIndex: 1,
		},
	}
	ok, err := s.KVSSetCAS(2, entry)
	if ok || err != nil {
		t.Fatalf("expected (false, nil), got: (%#v, %#v)", ok, err)
	}

	tx := s.db.Txn(false)
	if e, err := tx.First("kvs", "id", "foo"); e != nil || err != nil {
		t.Fatalf("expected (nil, nil), got: (%#v, %#v)", e, err)
	}
	tx.Abort()

	if idx := s.maxIndex("kvs"); idx != 0 {
		t.Fatalf("bad index: %d", idx)
	}

	entry = &structs.DirEntry{
		Key:   "foo",
		Value: []byte("foo"),
		RaftIndex: structs.RaftIndex{
			CreateIndex: 0,
			ModifyIndex: 0,
		},
	}
	ok, err = s.KVSSetCAS(2, entry)
	if !ok || err != nil {
		t.Fatalf("expected (true, nil), got: (%#v, %#v)", ok, err)
	}

	idx, entry, err := s.KVSGet(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if string(entry.Value) != "foo" || entry.CreateIndex != 2 || entry.ModifyIndex != 2 {
		t.Fatalf("bad entry: %#v", entry)
	}
	if idx != 2 {
		t.Fatalf("bad index: %d", idx)
	}

	entry = &structs.DirEntry{
		Key:   "foo",
		Value: []byte("foo"),
		RaftIndex: structs.RaftIndex{
			CreateIndex: 0,
			ModifyIndex: 0,
		},
	}
	ok, err = s.KVSSetCAS(3, entry)
	if ok || err != nil {
		t.Fatalf("expected (false, nil), got: (%#v, %#v)", ok, err)
	}

	entry = &structs.DirEntry{
		Key:   "foo",
		Value: []byte("bar"),
		RaftIndex: structs.RaftIndex{
			CreateIndex: 3,
			ModifyIndex: 3,
		},
	}
	ok, err = s.KVSSetCAS(3, entry)
	if ok || err != nil {
		t.Fatalf("expected (false, nil), got: (%#v, %#v)", ok, err)
	}

	idx, entry, err = s.KVSGet(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if string(entry.Value) != "foo" || entry.CreateIndex != 2 || entry.ModifyIndex != 2 {
		t.Fatalf("bad entry: %#v", entry)
	}
	if idx != 2 {
		t.Fatalf("bad index: %d", idx)
	}

	entry = &structs.DirEntry{
		Key:   "foo",
		Value: []byte("bar"),
		RaftIndex: structs.RaftIndex{
			CreateIndex: 2,
			ModifyIndex: 2,
		},
	}
	ok, err = s.KVSSetCAS(3, entry)
	if !ok || err != nil {
		t.Fatalf("expected (true, nil), got: (%#v, %#v)", ok, err)
	}

	idx, entry, err = s.KVSGet(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if string(entry.Value) != "bar" || entry.CreateIndex != 2 || entry.ModifyIndex != 3 {
		t.Fatalf("bad entry: %#v", entry)
	}
	if idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}

	entry = &structs.DirEntry{
		Key:     "foo",
		Value:   []byte("zoo"),
		Session: "nope",
		RaftIndex: structs.RaftIndex{
			CreateIndex: 2,
			ModifyIndex: 3,
		},
	}
	ok, err = s.KVSSetCAS(4, entry)
	if !ok || err != nil {
		t.Fatalf("expected (true, nil), got: (%#v, %#v)", ok, err)
	}

	idx, entry, err = s.KVSGet(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if string(entry.Value) != "zoo" || entry.CreateIndex != 2 || entry.ModifyIndex != 4 ||
		entry.Session != "" {
		t.Fatalf("bad entry: %#v", entry)
	}
	if idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}

	testRegisterNode(t, s, 5, "node1")
	session := testUUID()
	if err := s.SessionCreate(6, &structs.Session{ID: session, Node: "node1"}); err != nil {
		t.Fatalf("err: %s", err)
	}
	entry = &structs.DirEntry{
		Key:     "foo",
		Value:   []byte("locked"),
		Session: session,
		RaftIndex: structs.RaftIndex{
			CreateIndex: 2,
			ModifyIndex: 4,
		},
	}
	ok, err = s.KVSLock(6, entry)
	if !ok || err != nil {
		t.Fatalf("didn't get the lock: %v %s", ok, err)
	}
	entry = &structs.DirEntry{
		Key:   "foo",
		Value: []byte("locked"),
		RaftIndex: structs.RaftIndex{
			CreateIndex: 2,
			ModifyIndex: 6,
		},
	}
	ok, err = s.KVSSetCAS(7, entry)
	if !ok || err != nil {
		t.Fatalf("expected (true, nil), got: (%#v, %#v)", ok, err)
	}

	idx, entry, err = s.KVSGet(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if string(entry.Value) != "locked" || entry.CreateIndex != 2 || entry.ModifyIndex != 7 ||
		entry.Session != session {
		t.Fatalf("bad entry: %#v", entry)
	}
	if idx != 7 {
		t.Fatalf("bad index: %d", idx)
	}
}

func TestStateStore_KVSDeleteTree(t *testing.T) {
	s := testStateStore(t)

	testSetKey(t, s, 1, "foo/bar", "bar")
	testSetKey(t, s, 2, "foo/bar/baz", "baz")
	testSetKey(t, s, 3, "foo/bar/zip", "zip")
	testSetKey(t, s, 4, "foo/zorp", "zorp")

	if err := s.KVSDeleteTree(9, "bar"); err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx := s.maxIndex("kvs"); idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}

	if err := s.KVSDeleteTree(5, "foo/bar"); err != nil {
		t.Fatalf("err: %s", err)
	}

	tx := s.db.Txn(false)
	defer tx.Abort()

	entries, err := tx.Get("kvs", "id")
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	num := 0
	for entry := entries.Next(); entry != nil; entry = entries.Next() {
		if entry.(*structs.DirEntry).Key != "foo/zorp" {
			t.Fatalf("unexpected kvs entry: %#v", entry)
		}
		num++
	}

	if num != 1 {
		t.Fatalf("expected 1 key, got: %d", num)
	}

	if idx := s.maxIndex("kvs"); idx != 5 {
		t.Fatalf("bad index: %d", idx)
	}

	idx, _, err := s.KVSList(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 5 {
		t.Fatalf("bad index: %d", idx)
	}

	if err := s.ReapTombstones(5); err != nil {
		t.Fatalf("err: %s", err)
	}
	idx, _, err = s.KVSList(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}
}

func TestStateStore_Watches_PrefixDelete(t *testing.T) {
	s := testStateStore(t)

	testSetKey(t, s, 1, "foo", "foo")
	testSetKey(t, s, 2, "foo/bar", "bar")
	testSetKey(t, s, 3, "foo/bar/zip", "zip")
	testSetKey(t, s, 4, "foo/bar/zip/zorp", "zorp")
	testSetKey(t, s, 5, "foo/bar/zip/zap", "zap")
	testSetKey(t, s, 6, "foo/nope", "nope")

	ws := memdb.NewWatchSet()
	got, _, err := s.KVSList(ws, "foo/bar")
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	var wantIndex uint64 = 5
	if got != wantIndex {
		t.Fatalf("bad index: %d, expected %d", wantIndex, got)
	}

	if err := s.KVSDeleteTree(7, "foo/bar/zip"); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	if !watchFired(ws) {
		t.Fatalf("expected watch to fire but it did not")
	}

	got, _, err = s.KVSList(ws, "foo/bar")
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	wantIndex = 7
	if got != wantIndex {
		t.Fatalf("bad index: %d, expected %d", got, wantIndex)
	}

	if !watchFired(ws) {
		t.Fatalf("expected watch to fire but it did not")
	}

	if err := s.ReapTombstones(wantIndex); err != nil {
		t.Fatalf("err: %s", err)
	}

	got, _, err = s.KVSList(nil, "foo/bar")
	wantIndex = 2
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if got != wantIndex {
		t.Fatalf("bad index: %d, expected %d", got, wantIndex)
	}

	testSetKey(t, s, 8, "some/other/key", "")

	wantIndex = 8
	ws = memdb.NewWatchSet()
	got, _, err = s.KVSList(ws, "foo/bar/baz")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if watchFired(ws) {
		t.Fatalf("Watch should not have fired")
	}
	if got != wantIndex {
		t.Fatalf("bad index: %d, expected %d", got, wantIndex)
	}

	got, _, err = s.KVSList(nil, "")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if got != wantIndex {
		t.Fatalf("bad index: %d, expected %d", got, wantIndex)
	}

	if err := s.KVSDeleteTree(9, ""); err != nil {
		t.Fatalf("unexpected err: %s", err)
	}
	wantIndex = 9
	got, _, err = s.KVSList(nil, "/foo/bar")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if got != wantIndex {
		t.Fatalf("bad index: %d, expected %d", got, wantIndex)
	}

}

func TestStateStore_KVSLockDelay(t *testing.T) {
	s := testStateStore(t)

	expires := s.KVSLockDelay("/not/there")
	if expires.After(time.Now()) {
		t.Fatalf("bad: %v", expires)
	}
}

func TestStateStore_KVSLock(t *testing.T) {
	s := testStateStore(t)

	ok, err := s.KVSLock(0, &structs.DirEntry{Key: "foo", Value: []byte("foo")})
	if ok || err == nil || !strings.Contains(err.Error(), "missing session") {
		t.Fatalf("didn't detect missing session: %v %s", ok, err)
	}

	ok, err = s.KVSLock(1, &structs.DirEntry{Key: "foo", Value: []byte("foo"), Session: testUUID()})
	if ok || err == nil || !strings.Contains(err.Error(), "invalid session") {
		t.Fatalf("didn't detect invalid session: %v %s", ok, err)
	}

	testRegisterNode(t, s, 2, "node1")
	session1 := testUUID()
	if err := s.SessionCreate(3, &structs.Session{ID: session1, Node: "node1"}); err != nil {
		t.Fatalf("err: %s", err)
	}

	ok, err = s.KVSLock(4, &structs.DirEntry{Key: "foo", Value: []byte("foo"), Session: session1})
	if !ok || err != nil {
		t.Fatalf("didn't get the lock: %v %s", ok, err)
	}

	idx, result, err := s.KVSGet(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result.LockIndex != 1 || result.CreateIndex != 4 || result.ModifyIndex != 4 ||
		string(result.Value) != "foo" {
		t.Fatalf("bad entry: %#v", result)
	}
	if idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}

	ok, err = s.KVSLock(5, &structs.DirEntry{Key: "foo", Value: []byte("bar"), Session: session1})
	if !ok || err != nil {
		t.Fatalf("didn't handle locking an already-locked key: %v %s", ok, err)
	}

	idx, result, err = s.KVSGet(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result.LockIndex != 1 || result.CreateIndex != 4 || result.ModifyIndex != 5 ||
		string(result.Value) != "bar" {
		t.Fatalf("bad entry: %#v", result)
	}
	if idx != 5 {
		t.Fatalf("bad index: %d", idx)
	}

	ok, err = s.KVSUnlock(6, &structs.DirEntry{Key: "foo", Value: []byte("baz"), Session: session1})
	if !ok || err != nil {
		t.Fatalf("didn't handle unlocking a locked key: %v %s", ok, err)
	}
	ok, err = s.KVSLock(7, &structs.DirEntry{Key: "foo", Value: []byte("zoo"), Session: session1})
	if !ok || err != nil {
		t.Fatalf("didn't get the lock: %v %s", ok, err)
	}

	idx, result, err = s.KVSGet(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result.LockIndex != 2 || result.CreateIndex != 4 || result.ModifyIndex != 7 ||
		string(result.Value) != "zoo" {
		t.Fatalf("bad entry: %#v", result)
	}
	if idx != 7 {
		t.Fatalf("bad index: %d", idx)
	}

	testSetKey(t, s, 8, "bar", "bar")
	ok, err = s.KVSLock(9, &structs.DirEntry{Key: "bar", Value: []byte("xxx"), Session: session1})
	if !ok || err != nil {
		t.Fatalf("didn't get the lock: %v %s", ok, err)
	}

	idx, result, err = s.KVSGet(nil, "bar")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result.LockIndex != 1 || result.CreateIndex != 8 || result.ModifyIndex != 9 ||
		string(result.Value) != "xxx" {
		t.Fatalf("bad entry: %#v", result)
	}
	if idx != 9 {
		t.Fatalf("bad index: %d", idx)
	}

	session2 := testUUID()
	if err := s.SessionCreate(10, &structs.Session{ID: session2, Node: "node1"}); err != nil {
		t.Fatalf("err: %s", err)
	}

	ok, err = s.KVSLock(11, &structs.DirEntry{Key: "bar", Value: []byte("nope"), Session: session2})
	if ok || err != nil {
		t.Fatalf("didn't handle locking an already-locked key: %v %s", ok, err)
	}

	idx, result, err = s.KVSGet(nil, "bar")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result.LockIndex != 1 || result.CreateIndex != 8 || result.ModifyIndex != 9 ||
		string(result.Value) != "xxx" {
		t.Fatalf("bad entry: %#v", result)
	}
	if idx != 9 {
		t.Fatalf("bad index: %d", idx)
	}
}

func TestStateStore_KVSUnlock(t *testing.T) {
	s := testStateStore(t)

	ok, err := s.KVSUnlock(0, &structs.DirEntry{Key: "foo", Value: []byte("bar")})
	if ok || err == nil || !strings.Contains(err.Error(), "missing session") {
		t.Fatalf("didn't detect missing session: %v %s", ok, err)
	}

	testRegisterNode(t, s, 1, "node1")
	session1 := testUUID()
	if err := s.SessionCreate(2, &structs.Session{ID: session1, Node: "node1"}); err != nil {
		t.Fatalf("err: %s", err)
	}

	ok, err = s.KVSUnlock(3, &structs.DirEntry{Key: "foo", Value: []byte("bar"), Session: session1})
	if ok || err != nil {
		t.Fatalf("didn't handle unlocking a missing key: %v %s", ok, err)
	}

	testSetKey(t, s, 4, "foo", "bar")
	ok, err = s.KVSUnlock(5, &structs.DirEntry{Key: "foo", Value: []byte("baz"), Session: session1})
	if ok || err != nil {
		t.Fatalf("didn't handle unlocking a non-locked key: %v %s", ok, err)
	}

	idx, result, err := s.KVSGet(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result.LockIndex != 0 || result.CreateIndex != 4 || result.ModifyIndex != 4 ||
		string(result.Value) != "bar" {
		t.Fatalf("bad entry: %#v", result)
	}
	if idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}

	ok, err = s.KVSLock(6, &structs.DirEntry{Key: "foo", Value: []byte("bar"), Session: session1})
	if !ok || err != nil {
		t.Fatalf("didn't get the lock: %v %s", ok, err)
	}

	session2 := testUUID()
	if err := s.SessionCreate(7, &structs.Session{ID: session2, Node: "node1"}); err != nil {
		t.Fatalf("err: %s", err)
	}
	ok, err = s.KVSUnlock(8, &structs.DirEntry{Key: "foo", Value: []byte("zoo"), Session: session2})
	if ok || err != nil {
		t.Fatalf("didn't handle unlocking with the wrong session: %v %s", ok, err)
	}

	idx, result, err = s.KVSGet(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result.LockIndex != 1 || result.CreateIndex != 4 || result.ModifyIndex != 6 ||
		string(result.Value) != "bar" {
		t.Fatalf("bad entry: %#v", result)
	}
	if idx != 6 {
		t.Fatalf("bad index: %d", idx)
	}

	ok, err = s.KVSUnlock(9, &structs.DirEntry{Key: "foo", Value: []byte("zoo"), Session: session1})
	if !ok || err != nil {
		t.Fatalf("didn't handle unlocking with the correct session: %v %s", ok, err)
	}

	idx, result, err = s.KVSGet(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result.LockIndex != 1 || result.CreateIndex != 4 || result.ModifyIndex != 9 ||
		string(result.Value) != "zoo" {
		t.Fatalf("bad entry: %#v", result)
	}
	if idx != 9 {
		t.Fatalf("bad index: %d", idx)
	}

	ok, err = s.KVSUnlock(10, &structs.DirEntry{Key: "foo", Value: []byte("nope"), Session: session1})
	if ok || err != nil {
		t.Fatalf("didn't handle unlocking with the previous session: %v %s", ok, err)
	}

	idx, result, err = s.KVSGet(nil, "foo")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if result.LockIndex != 1 || result.CreateIndex != 4 || result.ModifyIndex != 9 ||
		string(result.Value) != "zoo" {
		t.Fatalf("bad entry: %#v", result)
	}
	if idx != 9 {
		t.Fatalf("bad index: %d", idx)
	}
}

func TestStateStore_KVS_Snapshot_Restore(t *testing.T) {
	s := testStateStore(t)

	entries := structs.DirEntries{
		&structs.DirEntry{
			Key:   "aaa",
			Flags: 23,
			Value: []byte("hello"),
		},
		&structs.DirEntry{
			Key:   "bar/a",
			Value: []byte("one"),
		},
		&structs.DirEntry{
			Key:   "bar/b",
			Value: []byte("two"),
		},
		&structs.DirEntry{
			Key:   "bar/c",
			Value: []byte("three"),
		},
	}
	for i, entry := range entries {
		if err := s.KVSSet(uint64(i+1), entry); err != nil {
			t.Fatalf("err: %s", err)
		}
	}

	testRegisterNode(t, s, 5, "node1")
	session := testUUID()
	if err := s.SessionCreate(6, &structs.Session{ID: session, Node: "node1"}); err != nil {
		t.Fatalf("err: %s", err)
	}
	entries[3].Session = session
	if ok, err := s.KVSLock(7, entries[3]); !ok || err != nil {
		t.Fatalf("didn't get the lock: %v %s", ok, err)
	}

	entries[3].LockIndex = 1

	snap := s.Snapshot()
	defer snap.Close()

	if err := s.KVSSet(8, &structs.DirEntry{Key: "aaa", Value: []byte("nope")}); err != nil {
		t.Fatalf("err: %s", err)
	}

	if idx := snap.LastIndex(); idx != 7 {
		t.Fatalf("bad index: %d", idx)
	}
	iter, err := snap.KVs()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	var dump structs.DirEntries
	for entry := iter.Next(); entry != nil; entry = iter.Next() {
		dump = append(dump, entry.(*structs.DirEntry))
	}
	if !reflect.DeepEqual(dump, entries) {
		t.Fatalf("bad: %#v", dump)
	}

	func() {
		s := testStateStore(t)
		restore := s.Restore()
		for _, entry := range dump {
			if err := restore.KVS(entry); err != nil {
				t.Fatalf("err: %s", err)
			}
		}
		restore.Commit()

		idx, res, err := s.KVSList(nil, "")
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if idx != 7 {
			t.Fatalf("bad index: %d", idx)
		}
		if !reflect.DeepEqual(res, entries) {
			t.Fatalf("bad: %#v", res)
		}

		if idx := s.maxIndex("kvs"); idx != 7 {
			t.Fatalf("bad index: %d", idx)
		}
	}()
}

func TestStateStore_Tombstone_Snapshot_Restore(t *testing.T) {
	s := testStateStore(t)

	testSetKey(t, s, 1, "foo/bar", "bar")
	testSetKey(t, s, 2, "foo/bar/baz", "bar")
	testSetKey(t, s, 3, "foo/bar/zoo", "bar")
	if err := s.KVSDelete(4, "foo/bar"); err != nil {
		t.Fatalf("err: %s", err)
	}

	snap := s.Snapshot()
	defer snap.Close()

	if err := s.ReapTombstones(4); err != nil {
		t.Fatalf("err: %s", err)
	}
	idx, _, err := s.KVSList(nil, "foo/bar")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}

	stones, err := snap.Tombstones()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	var dump []*Tombstone
	for stone := stones.Next(); stone != nil; stone = stones.Next() {
		dump = append(dump, stone.(*Tombstone))
	}
	if len(dump) != 1 {
		t.Fatalf("bad %#v", dump)
	}
	stone := dump[0]
	if stone.Key != "foo/bar" || stone.Index != 4 {
		t.Fatalf("bad: %#v", stone)
	}

	func() {
		s := testStateStore(t)
		restore := s.Restore()
		for _, stone := range dump {
			if err := restore.Tombstone(stone); err != nil {
				t.Fatalf("err: %s", err)
			}
		}
		restore.Commit()

		idx, _, err := s.KVSList(nil, "foo/bar")
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if idx != 4 {
			t.Fatalf("bad index: %d", idx)
		}

		if err := s.ReapTombstones(4); err != nil {
			t.Fatalf("err: %s", err)
		}
		idx, _, err = s.KVSList(nil, "foo/bar")
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if idx != 4 {
			t.Fatalf("bad index: %d", idx)
		}

		snap := s.Snapshot()
		defer snap.Close()
		stones, err := snap.Tombstones()
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if stones.Next() != nil {
			t.Fatalf("unexpected extra tombstones")
		}
	}()
}
