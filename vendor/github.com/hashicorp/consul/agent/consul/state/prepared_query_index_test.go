package state

import (
	"testing"

	"github.com/hashicorp/consul/agent/structs"
)

var index PreparedQueryIndex

func TestPreparedQueryIndex_FromObject(t *testing.T) {

	if ok, _, err := index.FromObject(42); ok || err == nil {
		t.Fatalf("bad: ok=%v err=%v", ok, err)
	}

	wrapped := &queryWrapper{&structs.PreparedQuery{}, nil}
	if ok, _, err := index.FromObject(wrapped); ok || err != nil {
		t.Fatalf("bad: ok=%v err=%v", ok, err)
	}

	query := &structs.PreparedQuery{
		Template: structs.QueryTemplateOptions{
			Type: structs.QueryTemplateTypeNamePrefixMatch,
		},
	}
	ok, key, err := index.FromObject(&queryWrapper{query, nil})
	if !ok || err != nil {
		t.Fatalf("bad: ok=%v err=%v", ok, err)
	}
	if string(key) != "\x00" {
		t.Fatalf("bad: %#v", key)
	}

	query.Name = "hello"
	ok, key, err = index.FromObject(&queryWrapper{query, nil})
	if !ok || err != nil {
		t.Fatalf("bad: ok=%v err=%v", ok, err)
	}
	if string(key) != "\x00hello" {
		t.Fatalf("bad: %#v", key)
	}

	query.Name = "HELLO"
	ok, key, err = index.FromObject(&queryWrapper{query, nil})
	if !ok || err != nil {
		t.Fatalf("bad: ok=%v err=%v", ok, err)
	}
	if string(key) != "\x00hello" {
		t.Fatalf("bad: %#v", key)
	}
}

func TestPreparedQueryIndex_FromArgs(t *testing.T) {

	if _, err := index.FromArgs(42); err == nil {
		t.Fatalf("should be an error")
	}
	if _, err := index.FromArgs("hello", "world"); err == nil {
		t.Fatalf("should be an error")
	}

	if key, err := index.FromArgs(""); err != nil || string(key) != "\x00" {
		t.Fatalf("bad: key=%#v err=%v", key, err)
	}

	if key, err := index.FromArgs("hello"); err != nil ||
		string(key) != "\x00hello" {
		t.Fatalf("bad: key=%#v err=%v", key, err)
	}

	if key, err := index.FromArgs("HELLO"); err != nil ||
		string(key) != "\x00hello" {
		t.Fatalf("bad: key=%#v err=%v", key, err)
	}
}

func TestPreparedQueryIndex_PrefixFromArgs(t *testing.T) {

	if _, err := index.PrefixFromArgs(42); err == nil {
		t.Fatalf("should be an error")
	}
	if _, err := index.PrefixFromArgs("hello", "world"); err == nil {
		t.Fatalf("should be an error")
	}

	if key, err := index.PrefixFromArgs(""); err != nil || string(key) != "\x00" {
		t.Fatalf("bad: key=%#v err=%v", key, err)
	}

	if key, err := index.PrefixFromArgs("hello"); err != nil ||
		string(key) != "\x00hello" {
		t.Fatalf("bad: key=%#v err=%v", key, err)
	}

	if key, err := index.PrefixFromArgs("HELLO"); err != nil ||
		string(key) != "\x00hello" {
		t.Fatalf("bad: key=%#v err=%v", key, err)
	}
}
