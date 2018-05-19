package testutil

import (
	"testing"

	"github.com/hashicorp/consul/api"
)
func TestQuiet(t *testing.T) {
	s, err := NewQuietServer()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("server ip: %#v\n", s.HTTPAddr)
	c, err := api.NewClient(&api.Config{Address: s.HTTPAddr})
	if err != nil {
		t.Fatal(err)
	}

	t.Log("storing a test key")
	want := &api.KVPair{Key: "abc", Value: []byte("def")}
	_, err = c.KV().Put(want, nil)
	if err != nil {
		t.Fatal(err)
	}
	have, _, err := c.KV().Get("abc", nil)
	if err != nil {
		t.Fatal(err)
	}

	if have == nil || have.Value == nil || string(have.Value) != "def" {
		t.Fatalf("bad test key: have %#v, want %#v\n", have, want)
	}
}

func TestTestutil(t *testing.T) {
	s, err := NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("server ip: %#v\n", s.HTTPAddr)
	c, err := api.NewClient(&api.Config{Address: s.HTTPAddr})
	if err != nil {
		t.Fatal(err)
	}

	t.Log("storing a test key")
	want := &api.KVPair{Key: "abc", Value: []byte("def")}
	_, err = c.KV().Put(want, nil)
	if err != nil {
		t.Fatal(err)
	}
	have, _, err := c.KV().Get("abc", nil)
	if err != nil {
		t.Fatal(err)
	}

	if have == nil || have.Value == nil || string(have.Value) != "def" {
		t.Fatalf("bad test key: have %#v, want %#v\n", have, want)
	}
}

