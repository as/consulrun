package api

import (
	"reflect"
	"testing"

	"github.com/hashicorp/consul/testutil/retry"
)

func TestAPI_PreparedQuery(t *testing.T) {
	t.Parallel()
	c, s := makeClient(t)
	defer s.Stop()

	reg := &CatalogRegistration{
		Datacenter: "dc1",
		Node:       "foobar",
		Address:    "192.168.10.10",
		TaggedAddresses: map[string]string{
			"wan": "127.0.0.1",
		},
		NodeMeta: map[string]string{"somekey": "somevalue"},
		Service: &AgentService{
			ID:      "redis1",
			Service: "redis",
			Tags:    []string{"master", "v1"},
			Port:    8000,
		},
	}

	catalog := c.Catalog()
	retry.Run(t, func(r *retry.R) {
		if _, err := catalog.Register(reg, nil); err != nil {
			r.Fatal(err)
		}
		if _, _, err := catalog.Node("foobar", nil); err != nil {
			r.Fatal(err)
		}
	})

	def := &PreparedQueryDefinition{
		Name: "test",
		Service: ServiceQuery{
			Service:  "redis",
			NodeMeta: map[string]string{"somekey": "somevalue"},
		},
	}

	query := c.PreparedQuery()
	var err error
	def.ID, _, err = query.Create(def, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	defs, _, err := query.Get(def.ID, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if len(defs) != 1 || !reflect.DeepEqual(defs[0], def) {
		t.Fatalf("bad: %v", defs)
	}

	defs, _, err = query.List(nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if len(defs) != 1 || !reflect.DeepEqual(defs[0], def) {
		t.Fatalf("bad: %v", defs)
	}

	def.Name = "my-query"
	_, err = query.Update(def, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	defs, _, err = query.Get(def.ID, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if len(defs) != 1 || !reflect.DeepEqual(defs[0], def) {
		t.Fatalf("bad: %v", defs)
	}

	results, _, err := query.Execute(def.ID, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if len(results.Nodes) != 1 || results.Nodes[0].Node.Node != "foobar" {
		t.Fatalf("bad: %v", results)
	}
	if wan, ok := results.Nodes[0].Node.TaggedAddresses["wan"]; !ok || wan != "127.0.0.1" {
		t.Fatalf("bad: %v", results)
	}

	results, _, err = query.Execute("my-query", nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if len(results.Nodes) != 1 || results.Nodes[0].Node.Node != "foobar" {
		t.Fatalf("bad: %v", results)
	}
	if wan, ok := results.Nodes[0].Node.TaggedAddresses["wan"]; !ok || wan != "127.0.0.1" {
		t.Fatalf("bad: %v", results)
	}
	if results.Nodes[0].Node.Datacenter != "dc1" {
		t.Fatalf("bad datacenter: %v", results)
	}

	reg2 := reg
	reg2.Node = "failingnode"
	reg2.Check = &AgentCheck{
		Node:        "failingnode",
		ServiceID:   "redis1",
		ServiceName: "redis",
		Name:        "failingcheck",
		Status:      "critical",
	}
	retry.Run(t, func(r *retry.R) {
		if _, err := catalog.Register(reg2, nil); err != nil {
			r.Fatal(err)
		}
		if _, _, err := catalog.Node("failingnode", nil); err != nil {
			r.Fatal(err)
		}
	})

	results, _, err = query.Execute(def.ID, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if len(results.Nodes) != 1 || results.Nodes[0].Node.Node != "foobar" {
		t.Fatalf("bad: %v", results)
	}
	if wan, ok := results.Nodes[0].Node.TaggedAddresses["wan"]; !ok || wan != "127.0.0.1" {
		t.Fatalf("bad: %v", results)
	}

	def.Service.IgnoreCheckIDs = []string{"failingcheck"}
	_, err = query.Update(def, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	results, _, err = query.Execute(def.ID, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if len(results.Nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(results.Nodes))
	}

	_, err = query.Delete(def.ID, nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	defs, _, err = query.List(nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if len(defs) != 0 {
		t.Fatalf("bad: %v", defs)
	}
}
