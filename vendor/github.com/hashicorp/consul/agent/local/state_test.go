package local_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/consul/agent"
	"github.com/hashicorp/consul/agent/config"
	"github.com/hashicorp/consul/agent/local"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/agent/token"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/testutil/retry"
	"github.com/hashicorp/consul/types"
	"github.com/pascaldekloe/goe/verify"
)

func TestAgentAntiEntropy_Services(t *testing.T) {
	t.Parallel()
	a := &agent.TestAgent{Name: t.Name()}
	a.Start()
	defer a.Shutdown()

	args := &structs.RegisterRequest{
		Datacenter: "dc1",
		Node:       a.Config.NodeName,
		Address:    "127.0.0.1",
	}

	var out struct{}
	srv1 := &structs.NodeService{
		ID:      "mysql",
		Service: "mysql",
		Tags:    []string{"master"},
		Port:    5000,
	}
	a.State.AddService(srv1, "")
	args.Service = srv1
	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	srv2 := &structs.NodeService{
		ID:      "redis",
		Service: "redis",
		Tags:    []string{},
		Port:    8000,
	}
	a.State.AddService(srv2, "")

	srv2_mod := new(structs.NodeService)
	*srv2_mod = *srv2
	srv2_mod.Port = 9000
	args.Service = srv2_mod
	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	srv3 := &structs.NodeService{
		ID:      "web",
		Service: "web",
		Tags:    []string{},
		Port:    80,
	}
	a.State.AddService(srv3, "")

	srv4 := &structs.NodeService{
		ID:      "lb",
		Service: "lb",
		Tags:    []string{},
		Port:    443,
	}
	args.Service = srv4
	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	srv5 := &structs.NodeService{
		ID:      "api",
		Service: "api",
		Tags:    []string{},
		Address: "127.0.0.10",
		Port:    8000,
	}
	a.State.AddService(srv5, "")

	srv5_mod := new(structs.NodeService)
	*srv5_mod = *srv5
	srv5_mod.Address = "127.0.0.1"
	args.Service = srv5_mod
	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	srv6 := &structs.NodeService{
		ID:      "cache",
		Service: "cache",
		Tags:    []string{},
		Port:    11211,
	}
	a.State.SetServiceState(&local.ServiceState{
		Service: srv6,
		InSync:  true,
	})

	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	var services structs.IndexedNodeServices
	req := structs.NodeSpecificRequest{
		Datacenter: "dc1",
		Node:       a.Config.NodeName,
	}

	if err := a.RPC("Catalog.NodeServices", &req, &services); err != nil {
		t.Fatalf("err: %v", err)
	}

	id := services.NodeServices.Node.ID
	addrs := services.NodeServices.Node.TaggedAddresses
	meta := services.NodeServices.Node.Meta
	delete(meta, structs.MetaSegmentKey) // Added later, not in config.
	verify.Values(t, "node id", id, a.Config.NodeID)
	verify.Values(t, "tagged addrs", addrs, a.Config.TaggedAddresses)
	verify.Values(t, "node meta", meta, a.Config.NodeMeta)

	if len(services.NodeServices.Services) != 6 {
		t.Fatalf("bad: %v", services.NodeServices.Services)
	}

	for id, serv := range services.NodeServices.Services {
		serv.CreateIndex, serv.ModifyIndex = 0, 0
		switch id {
		case "mysql":
			if !reflect.DeepEqual(serv, srv1) {
				t.Fatalf("bad: %v %v", serv, srv1)
			}
		case "redis":
			if !reflect.DeepEqual(serv, srv2) {
				t.Fatalf("bad: %#v %#v", serv, srv2)
			}
		case "web":
			if !reflect.DeepEqual(serv, srv3) {
				t.Fatalf("bad: %v %v", serv, srv3)
			}
		case "api":
			if !reflect.DeepEqual(serv, srv5) {
				t.Fatalf("bad: %v %v", serv, srv5)
			}
		case "cache":
			if !reflect.DeepEqual(serv, srv6) {
				t.Fatalf("bad: %v %v", serv, srv6)
			}
		case structs.ConsulServiceID:

		default:
			t.Fatalf("unexpected service: %v", id)
		}
	}

	if err := servicesInSync(a.State, 5); err != nil {
		t.Fatal(err)
	}

	a.State.RemoveService("api")

	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := a.RPC("Catalog.NodeServices", &req, &services); err != nil {
		t.Fatalf("err: %v", err)
	}

	if len(services.NodeServices.Services) != 5 {
		t.Fatalf("bad: %v", services.NodeServices.Services)
	}

	for id, serv := range services.NodeServices.Services {
		serv.CreateIndex, serv.ModifyIndex = 0, 0
		switch id {
		case "mysql":
			if !reflect.DeepEqual(serv, srv1) {
				t.Fatalf("bad: %v %v", serv, srv1)
			}
		case "redis":
			if !reflect.DeepEqual(serv, srv2) {
				t.Fatalf("bad: %#v %#v", serv, srv2)
			}
		case "web":
			if !reflect.DeepEqual(serv, srv3) {
				t.Fatalf("bad: %v %v", serv, srv3)
			}
		case "cache":
			if !reflect.DeepEqual(serv, srv6) {
				t.Fatalf("bad: %v %v", serv, srv6)
			}
		case structs.ConsulServiceID:

		default:
			t.Fatalf("unexpected service: %v", id)
		}
	}

	if err := servicesInSync(a.State, 4); err != nil {
		t.Fatal(err)
	}
}

func TestAgentAntiEntropy_EnableTagOverride(t *testing.T) {
	t.Parallel()
	a := &agent.TestAgent{Name: t.Name()}
	a.Start()
	defer a.Shutdown()

	args := &structs.RegisterRequest{
		Datacenter: "dc1",
		Node:       a.Config.NodeName,
		Address:    "127.0.0.1",
	}
	var out struct{}

	srv1 := &structs.NodeService{
		ID:                "svc_id1",
		Service:           "svc1",
		Tags:              []string{"tag1"},
		Port:              6100,
		EnableTagOverride: true,
	}
	a.State.AddService(srv1, "")

	srv2 := &structs.NodeService{
		ID:                "svc_id2",
		Service:           "svc2",
		Tags:              []string{"tag2"},
		Port:              6200,
		EnableTagOverride: false,
	}
	a.State.AddService(srv2, "")

	if err := a.State.SyncChanges(); err != nil {
		t.Fatalf("err: %v", err)
	}

	args.Service = &structs.NodeService{
		ID:                srv1.ID,
		Service:           srv1.Service,
		Tags:              []string{"tag1_mod"},
		Port:              7100,
		EnableTagOverride: true,
	}
	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	args.Service = &structs.NodeService{
		ID:                srv2.ID,
		Service:           srv2.Service,
		Tags:              []string{"tag2_mod"},
		Port:              7200,
		EnableTagOverride: false,
	}
	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	req := structs.NodeSpecificRequest{
		Datacenter: "dc1",
		Node:       a.Config.NodeName,
	}
	var services structs.IndexedNodeServices

	if err := a.RPC("Catalog.NodeServices", &req, &services); err != nil {
		t.Fatalf("err: %v", err)
	}

	for id, serv := range services.NodeServices.Services {
		serv.CreateIndex, serv.ModifyIndex = 0, 0
		switch id {
		case "svc_id1":

			got := serv
			want := &structs.NodeService{
				ID:                "svc_id1",
				Service:           "svc1",
				Tags:              []string{"tag1_mod"},
				Port:              6100,
				EnableTagOverride: true,
			}
			if !verify.Values(t, "", got, want) {
				t.FailNow()
			}
		case "svc_id2":
			got, want := serv, srv2
			if !verify.Values(t, "", got, want) {
				t.FailNow()
			}
		case structs.ConsulServiceID:

		default:
			t.Fatalf("unexpected service: %v", id)
		}
	}

	if err := servicesInSync(a.State, 2); err != nil {
		t.Fatal(err)
	}
}

func TestAgentAntiEntropy_Services_WithChecks(t *testing.T) {
	t.Parallel()
	a := agent.NewTestAgent(t.Name(), "")
	defer a.Shutdown()

	{

		srv := &structs.NodeService{
			ID:      "mysql",
			Service: "mysql",
			Tags:    []string{"master"},
			Port:    5000,
		}
		a.State.AddService(srv, "")

		chk := &structs.HealthCheck{
			Node:      a.Config.NodeName,
			CheckID:   "mysql",
			Name:      "mysql",
			ServiceID: "mysql",
			Status:    api.HealthPassing,
		}
		a.State.AddCheck(chk, "")

		if err := a.State.SyncFull(); err != nil {
			t.Fatal("sync failed: ", err)
		}

		svcReq := structs.NodeSpecificRequest{
			Datacenter: "dc1",
			Node:       a.Config.NodeName,
		}
		var services structs.IndexedNodeServices
		if err := a.RPC("Catalog.NodeServices", &svcReq, &services); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(services.NodeServices.Services) != 2 {
			t.Fatalf("bad: %v", services.NodeServices.Services)
		}

		chkReq := structs.ServiceSpecificRequest{
			Datacenter:  "dc1",
			ServiceName: "mysql",
		}
		var checks structs.IndexedHealthChecks
		if err := a.RPC("Health.ServiceChecks", &chkReq, &checks); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(checks.HealthChecks) != 1 {
			t.Fatalf("bad: %v", checks)
		}
	}

	{

		srv := &structs.NodeService{
			ID:      "redis",
			Service: "redis",
			Tags:    []string{"master"},
			Port:    5000,
		}
		a.State.AddService(srv, "")

		chk1 := &structs.HealthCheck{
			Node:      a.Config.NodeName,
			CheckID:   "redis:1",
			Name:      "redis:1",
			ServiceID: "redis",
			Status:    api.HealthPassing,
		}
		a.State.AddCheck(chk1, "")

		chk2 := &structs.HealthCheck{
			Node:      a.Config.NodeName,
			CheckID:   "redis:2",
			Name:      "redis:2",
			ServiceID: "redis",
			Status:    api.HealthPassing,
		}
		a.State.AddCheck(chk2, "")

		if err := a.State.SyncFull(); err != nil {
			t.Fatal("sync failed: ", err)
		}

		svcReq := structs.NodeSpecificRequest{
			Datacenter: "dc1",
			Node:       a.Config.NodeName,
		}
		var services structs.IndexedNodeServices
		if err := a.RPC("Catalog.NodeServices", &svcReq, &services); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(services.NodeServices.Services) != 3 {
			t.Fatalf("bad: %v", services.NodeServices.Services)
		}

		chkReq := structs.ServiceSpecificRequest{
			Datacenter:  "dc1",
			ServiceName: "redis",
		}
		var checks structs.IndexedHealthChecks
		if err := a.RPC("Health.ServiceChecks", &chkReq, &checks); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(checks.HealthChecks) != 2 {
			t.Fatalf("bad: %v", checks)
		}
	}
}

var testRegisterRules = `
 node "" {
 	policy = "write"
 }

 service "api" {
 	policy = "write"
 }

 service "consul" {
 	policy = "write"
 }
 `

func TestAgentAntiEntropy_Services_ACLDeny(t *testing.T) {
	t.Parallel()
	a := &agent.TestAgent{Name: t.Name(), HCL: `
		acl_datacenter = "dc1"
		acl_master_token = "root"
		acl_default_policy = "deny"
		acl_enforce_version_8 = true`}
	a.Start()
	defer a.Shutdown()

	arg := structs.ACLRequest{
		Datacenter: "dc1",
		Op:         structs.ACLSet,
		ACL: structs.ACL{
			Name:  "User token",
			Type:  structs.ACLTypeClient,
			Rules: testRegisterRules,
		},
		WriteRequest: structs.WriteRequest{
			Token: "root",
		},
	}
	var token string
	if err := a.RPC("ACL.Apply", &arg, &token); err != nil {
		t.Fatalf("err: %v", err)
	}

	srv1 := &structs.NodeService{
		ID:      "mysql",
		Service: "mysql",
		Tags:    []string{"master"},
		Port:    5000,
	}
	a.State.AddService(srv1, token)

	srv2 := &structs.NodeService{
		ID:      "api",
		Service: "api",
		Tags:    []string{"foo"},
		Port:    5001,
	}
	a.State.AddService(srv2, token)

	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.NodeSpecificRequest{
			Datacenter: "dc1",
			Node:       a.Config.NodeName,
			QueryOptions: structs.QueryOptions{
				Token: "root",
			},
		}
		var services structs.IndexedNodeServices
		if err := a.RPC("Catalog.NodeServices", &req, &services); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(services.NodeServices.Services) != 2 {
			t.Fatalf("bad: %v", services.NodeServices.Services)
		}

		for id, serv := range services.NodeServices.Services {
			serv.CreateIndex, serv.ModifyIndex = 0, 0
			switch id {
			case "mysql":
				t.Fatalf("should not be permitted")
			case "api":
				if !reflect.DeepEqual(serv, srv2) {
					t.Fatalf("bad: %#v %#v", serv, srv2)
				}
			case structs.ConsulServiceID:

			default:
				t.Fatalf("unexpected service: %v", id)
			}
		}

		if err := servicesInSync(a.State, 2); err != nil {
			t.Fatal(err)
		}
	}

	a.State.RemoveService("api")
	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.NodeSpecificRequest{
			Datacenter: "dc1",
			Node:       a.Config.NodeName,
			QueryOptions: structs.QueryOptions{
				Token: "root",
			},
		}
		var services structs.IndexedNodeServices
		if err := a.RPC("Catalog.NodeServices", &req, &services); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(services.NodeServices.Services) != 1 {
			t.Fatalf("bad: %v", services.NodeServices.Services)
		}

		for id, serv := range services.NodeServices.Services {
			serv.CreateIndex, serv.ModifyIndex = 0, 0
			switch id {
			case "mysql":
				t.Fatalf("should not be permitted")
			case "api":
				t.Fatalf("should be deleted")
			case structs.ConsulServiceID:

			default:
				t.Fatalf("unexpected service: %v", id)
			}
		}

		if err := servicesInSync(a.State, 1); err != nil {
			t.Fatal(err)
		}
	}

	if token := a.State.ServiceToken("api"); token != "" {
		t.Fatalf("bad: %s", token)
	}
}

func TestAgentAntiEntropy_Checks(t *testing.T) {
	t.Parallel()
	a := &agent.TestAgent{Name: t.Name()}
	a.Start()
	defer a.Shutdown()

	args := &structs.RegisterRequest{
		Datacenter: "dc1",
		Node:       a.Config.NodeName,
		Address:    "127.0.0.1",
	}

	var out struct{}
	chk1 := &structs.HealthCheck{
		Node:    a.Config.NodeName,
		CheckID: "mysql",
		Name:    "mysql",
		Status:  api.HealthPassing,
	}
	a.State.AddCheck(chk1, "")
	args.Check = chk1
	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	chk2 := &structs.HealthCheck{
		Node:    a.Config.NodeName,
		CheckID: "redis",
		Name:    "redis",
		Status:  api.HealthPassing,
	}
	a.State.AddCheck(chk2, "")

	chk2_mod := new(structs.HealthCheck)
	*chk2_mod = *chk2
	chk2_mod.Status = api.HealthCritical
	args.Check = chk2_mod
	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	chk3 := &structs.HealthCheck{
		Node:    a.Config.NodeName,
		CheckID: "web",
		Name:    "web",
		Status:  api.HealthPassing,
	}
	a.State.AddCheck(chk3, "")

	chk4 := &structs.HealthCheck{
		Node:    a.Config.NodeName,
		CheckID: "lb",
		Name:    "lb",
		Status:  api.HealthPassing,
	}
	args.Check = chk4
	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	chk5 := &structs.HealthCheck{
		Node:    a.Config.NodeName,
		CheckID: "cache",
		Name:    "cache",
		Status:  api.HealthPassing,
	}
	a.State.SetCheckState(&local.CheckState{
		Check:  chk5,
		InSync: true,
	})

	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	req := structs.NodeSpecificRequest{
		Datacenter: "dc1",
		Node:       a.Config.NodeName,
	}
	var checks structs.IndexedHealthChecks

	if err := a.RPC("Health.NodeChecks", &req, &checks); err != nil {
		t.Fatalf("err: %v", err)
	}

	if len(checks.HealthChecks) != 5 {
		t.Fatalf("bad: %v", checks)
	}

	for _, chk := range checks.HealthChecks {
		chk.CreateIndex, chk.ModifyIndex = 0, 0
		switch chk.CheckID {
		case "mysql":
			if !reflect.DeepEqual(chk, chk1) {
				t.Fatalf("bad: %v %v", chk, chk1)
			}
		case "redis":
			if !reflect.DeepEqual(chk, chk2) {
				t.Fatalf("bad: %v %v", chk, chk2)
			}
		case "web":
			if !reflect.DeepEqual(chk, chk3) {
				t.Fatalf("bad: %v %v", chk, chk3)
			}
		case "cache":
			if !reflect.DeepEqual(chk, chk5) {
				t.Fatalf("bad: %v %v", chk, chk5)
			}
		case "serfHealth":

		default:
			t.Fatalf("unexpected check: %v", chk)
		}
	}

	if err := checksInSync(a.State, 4); err != nil {
		t.Fatal(err)
	}

	{
		req := structs.NodeSpecificRequest{
			Datacenter: "dc1",
			Node:       a.Config.NodeName,
		}
		var services structs.IndexedNodeServices
		if err := a.RPC("Catalog.NodeServices", &req, &services); err != nil {
			t.Fatalf("err: %v", err)
		}

		id := services.NodeServices.Node.ID
		addrs := services.NodeServices.Node.TaggedAddresses
		meta := services.NodeServices.Node.Meta
		delete(meta, structs.MetaSegmentKey) // Added later, not in config.
		verify.Values(t, "node id", id, a.Config.NodeID)
		verify.Values(t, "tagged addrs", addrs, a.Config.TaggedAddresses)
		verify.Values(t, "node meta", meta, a.Config.NodeMeta)
	}

	a.State.RemoveCheck("redis")

	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := a.RPC("Health.NodeChecks", &req, &checks); err != nil {
		t.Fatalf("err: %v", err)
	}

	if len(checks.HealthChecks) != 4 {
		t.Fatalf("bad: %v", checks)
	}

	for _, chk := range checks.HealthChecks {
		chk.CreateIndex, chk.ModifyIndex = 0, 0
		switch chk.CheckID {
		case "mysql":
			if !reflect.DeepEqual(chk, chk1) {
				t.Fatalf("bad: %v %v", chk, chk1)
			}
		case "web":
			if !reflect.DeepEqual(chk, chk3) {
				t.Fatalf("bad: %v %v", chk, chk3)
			}
		case "cache":
			if !reflect.DeepEqual(chk, chk5) {
				t.Fatalf("bad: %v %v", chk, chk5)
			}
		case "serfHealth":

		default:
			t.Fatalf("unexpected check: %v", chk)
		}
	}

	if err := checksInSync(a.State, 3); err != nil {
		t.Fatal(err)
	}
}

func TestAgentAntiEntropy_Checks_ACLDeny(t *testing.T) {
	t.Parallel()
	a := &agent.TestAgent{Name: t.Name(), HCL: `
		acl_datacenter = "dc1"
		acl_master_token = "root"
		acl_default_policy = "deny"
		acl_enforce_version_8 = true`}
	a.Start()
	defer a.Shutdown()

	arg := structs.ACLRequest{
		Datacenter: "dc1",
		Op:         structs.ACLSet,
		ACL: structs.ACL{
			Name:  "User token",
			Type:  structs.ACLTypeClient,
			Rules: testRegisterRules,
		},
		WriteRequest: structs.WriteRequest{
			Token: "root",
		},
	}
	var token string
	if err := a.RPC("ACL.Apply", &arg, &token); err != nil {
		t.Fatalf("err: %v", err)
	}

	srv1 := &structs.NodeService{
		ID:      "mysql",
		Service: "mysql",
		Tags:    []string{"master"},
		Port:    5000,
	}
	a.State.AddService(srv1, "root")
	srv2 := &structs.NodeService{
		ID:      "api",
		Service: "api",
		Tags:    []string{"foo"},
		Port:    5001,
	}
	a.State.AddService(srv2, "root")

	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.NodeSpecificRequest{
			Datacenter: "dc1",
			Node:       a.Config.NodeName,
			QueryOptions: structs.QueryOptions{
				Token: "root",
			},
		}
		var services structs.IndexedNodeServices
		if err := a.RPC("Catalog.NodeServices", &req, &services); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(services.NodeServices.Services) != 3 {
			t.Fatalf("bad: %v", services.NodeServices.Services)
		}

		for id, serv := range services.NodeServices.Services {
			serv.CreateIndex, serv.ModifyIndex = 0, 0
			switch id {
			case "mysql":
				if !reflect.DeepEqual(serv, srv1) {
					t.Fatalf("bad: %#v %#v", serv, srv1)
				}
			case "api":
				if !reflect.DeepEqual(serv, srv2) {
					t.Fatalf("bad: %#v %#v", serv, srv2)
				}
			case structs.ConsulServiceID:

			default:
				t.Fatalf("unexpected service: %v", id)
			}
		}

		if err := servicesInSync(a.State, 2); err != nil {
			t.Fatal(err)
		}
	}

	chk1 := &structs.HealthCheck{
		Node:        a.Config.NodeName,
		ServiceID:   "mysql",
		ServiceName: "mysql",
		ServiceTags: []string{"master"},
		CheckID:     "mysql-check",
		Name:        "mysql",
		Status:      api.HealthPassing,
	}
	a.State.AddCheck(chk1, token)

	chk2 := &structs.HealthCheck{
		Node:        a.Config.NodeName,
		ServiceID:   "api",
		ServiceName: "api",
		ServiceTags: []string{"foo"},
		CheckID:     "api-check",
		Name:        "api",
		Status:      api.HealthPassing,
	}
	a.State.AddCheck(chk2, token)

	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	req := structs.NodeSpecificRequest{
		Datacenter: "dc1",
		Node:       a.Config.NodeName,
		QueryOptions: structs.QueryOptions{
			Token: "root",
		},
	}
	var checks structs.IndexedHealthChecks
	if err := a.RPC("Health.NodeChecks", &req, &checks); err != nil {
		t.Fatalf("err: %v", err)
	}

	if len(checks.HealthChecks) != 2 {
		t.Fatalf("bad: %v", checks)
	}

	for _, chk := range checks.HealthChecks {
		chk.CreateIndex, chk.ModifyIndex = 0, 0
		switch chk.CheckID {
		case "mysql-check":
			t.Fatalf("should not be permitted")
		case "api-check":
			if !reflect.DeepEqual(chk, chk2) {
				t.Fatalf("bad: %v %v", chk, chk2)
			}
		case "serfHealth":

		default:
			t.Fatalf("unexpected check: %v", chk)
		}
	}

	if err := checksInSync(a.State, 2); err != nil {
		t.Fatal(err)
	}

	a.State.RemoveCheck("api-check")
	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.NodeSpecificRequest{
			Datacenter: "dc1",
			Node:       a.Config.NodeName,
			QueryOptions: structs.QueryOptions{
				Token: "root",
			},
		}
		var checks structs.IndexedHealthChecks
		if err := a.RPC("Health.NodeChecks", &req, &checks); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(checks.HealthChecks) != 1 {
			t.Fatalf("bad: %v", checks)
		}

		for _, chk := range checks.HealthChecks {
			chk.CreateIndex, chk.ModifyIndex = 0, 0
			switch chk.CheckID {
			case "mysql-check":
				t.Fatalf("should not be permitted")
			case "api-check":
				t.Fatalf("should be deleted")
			case "serfHealth":

			default:
				t.Fatalf("unexpected check: %v", chk)
			}
		}
	}

	if err := checksInSync(a.State, 1); err != nil {
		t.Fatal(err)
	}

	if token := a.State.CheckToken("api-check"); token != "" {
		t.Fatalf("bad: %s", token)
	}
}

func TestAgent_UpdateCheck_DiscardOutput(t *testing.T) {
	t.Parallel()
	a := agent.NewTestAgent(t.Name(), `
		discard_check_output = true
		check_update_interval = "0s" # set to "0s" since otherwise output checks are deferred
	`)
	defer a.Shutdown()

	inSync := func(id string) bool {
		s := a.State.CheckState(types.CheckID(id))
		if s == nil {
			return false
		}
		return s.InSync
	}

	check := &structs.HealthCheck{
		Node:    a.Config.NodeName,
		CheckID: "web",
		Name:    "web",
		Status:  api.HealthPassing,
		Output:  "first output",
	}
	if err := a.State.AddCheck(check, ""); err != nil {
		t.Fatalf("bad: %s", err)
	}
	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("bad: %s", err)
	}
	if !inSync("web") {
		t.Fatal("check should be in sync")
	}

	a.State.UpdateCheck(check.CheckID, api.HealthPassing, "second output")
	if !inSync("web") {
		t.Fatal("check should be in sync")
	}

	a.State.SetDiscardCheckOutput(false)
	a.State.UpdateCheck(check.CheckID, api.HealthPassing, "third output")
	if inSync("web") {
		t.Fatal("check should be out of sync")
	}
}

func TestAgentAntiEntropy_Check_DeferSync(t *testing.T) {
	t.Parallel()
	a := &agent.TestAgent{Name: t.Name(), HCL: `
		check_update_interval = "500ms"
	`}
	a.Start()
	defer a.Shutdown()

	check := &structs.HealthCheck{
		Node:    a.Config.NodeName,
		CheckID: "web",
		Name:    "web",
		Status:  api.HealthPassing,
		Output:  "",
	}
	a.State.AddCheck(check, "")

	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	req := structs.NodeSpecificRequest{
		Datacenter: "dc1",
		Node:       a.Config.NodeName,
	}
	var checks structs.IndexedHealthChecks
	retry.Run(t, func(r *retry.R) {
		if err := a.RPC("Health.NodeChecks", &req, &checks); err != nil {
			r.Fatalf("err: %v", err)
		}
		if got, want := len(checks.HealthChecks), 2; got != want {
			r.Fatalf("got %d health checks want %d", got, want)
		}
	})

	a.State.UpdateCheck("web", api.HealthPassing, "output")

	time.Sleep(250 * time.Millisecond)
	if err := a.RPC("Health.NodeChecks", &req, &checks); err != nil {
		t.Fatalf("err: %v", err)
	}

	for _, chk := range checks.HealthChecks {
		switch chk.CheckID {
		case "web":
			if chk.Output != "" {
				t.Fatalf("early update: %v", chk)
			}
		}
	}

	timer := &retry.Timer{Timeout: 6 * time.Second, Wait: 100 * time.Millisecond}
	retry.RunWith(timer, t, func(r *retry.R) {
		if err := a.RPC("Health.NodeChecks", &req, &checks); err != nil {
			r.Fatal(err)
		}

		for _, chk := range checks.HealthChecks {
			switch chk.CheckID {
			case "web":
				if chk.Output != "output" {
					r.Fatalf("no update: %v", chk)
				}
			}
		}
	})

	eCopy := check.Clone()
	eCopy.Output = "changed"
	reg := structs.RegisterRequest{
		Datacenter:      a.Config.Datacenter,
		Node:            a.Config.NodeName,
		Address:         a.Config.AdvertiseAddrLAN.IP.String(),
		TaggedAddresses: a.Config.TaggedAddresses,
		Check:           eCopy,
		WriteRequest:    structs.WriteRequest{},
	}
	var out struct{}
	if err := a.RPC("Catalog.Register", &reg, &out); err != nil {
		t.Fatalf("err: %s", err)
	}

	if err := a.RPC("Health.NodeChecks", &req, &checks); err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, chk := range checks.HealthChecks {
		switch chk.CheckID {
		case "web":
			if chk.Output != "changed" {
				t.Fatalf("unexpected update: %v", chk)
			}
		}
	}

	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := a.RPC("Health.NodeChecks", &req, &checks); err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, chk := range checks.HealthChecks {
		switch chk.CheckID {
		case "web":
			if chk.Output != "output" {
				t.Fatalf("missed update: %v", chk)
			}
		}
	}

	if err := a.RPC("Catalog.Register", &reg, &out); err != nil {
		t.Fatalf("err: %s", err)
	}

	if err := a.RPC("Health.NodeChecks", &req, &checks); err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, chk := range checks.HealthChecks {
		switch chk.CheckID {
		case "web":
			if chk.Output != "changed" {
				t.Fatalf("unexpected update: %v", chk)
			}
		}
	}

	a.State.UpdateCheck("web", api.HealthPassing, "deferred")

	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := a.RPC("Health.NodeChecks", &req, &checks); err != nil {
		t.Fatalf("err: %v", err)
	}
	for _, chk := range checks.HealthChecks {
		switch chk.CheckID {
		case "web":
			if chk.Output != "changed" {
				t.Fatalf("unexpected update: %v", chk)
			}
		}
	}

	retry.Run(t, func(r *retry.R) {
		if err := a.RPC("Health.NodeChecks", &req, &checks); err != nil {
			r.Fatal(err)
		}

		for _, chk := range checks.HealthChecks {
			switch chk.CheckID {
			case "web":
				if chk.Output != "deferred" {
					r.Fatalf("no update: %v", chk)
				}
			}
		}
	})

}

func TestAgentAntiEntropy_NodeInfo(t *testing.T) {
	t.Parallel()
	nodeID := types.NodeID("40e4a748-2192-161a-0510-9bf59fe950b5")
	nodeMeta := map[string]string{
		"somekey": "somevalue",
	}
	a := &agent.TestAgent{Name: t.Name(), HCL: `
		node_id = "40e4a748-2192-161a-0510-9bf59fe950b5"
		node_meta {
			somekey = "somevalue"
		}`}
	a.Start()
	defer a.Shutdown()

	args := &structs.RegisterRequest{
		Datacenter: "dc1",
		Node:       a.Config.NodeName,
		Address:    "127.0.0.1",
	}
	var out struct{}
	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	req := structs.NodeSpecificRequest{
		Datacenter: "dc1",
		Node:       a.Config.NodeName,
	}
	var services structs.IndexedNodeServices
	if err := a.RPC("Catalog.NodeServices", &req, &services); err != nil {
		t.Fatalf("err: %v", err)
	}

	id := services.NodeServices.Node.ID
	addrs := services.NodeServices.Node.TaggedAddresses
	meta := services.NodeServices.Node.Meta
	delete(meta, structs.MetaSegmentKey) // Added later, not in config.
	if id != a.Config.NodeID ||
		!reflect.DeepEqual(addrs, a.Config.TaggedAddresses) ||
		!reflect.DeepEqual(meta, a.Config.NodeMeta) {
		t.Fatalf("bad: %v", services.NodeServices.Node)
	}

	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := a.State.SyncFull(); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := a.RPC("Catalog.NodeServices", &req, &services); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		id := services.NodeServices.Node.ID
		addrs := services.NodeServices.Node.TaggedAddresses
		meta := services.NodeServices.Node.Meta
		delete(meta, structs.MetaSegmentKey) // Added later, not in config.
		if id != nodeID ||
			!reflect.DeepEqual(addrs, a.Config.TaggedAddresses) ||
			!reflect.DeepEqual(meta, nodeMeta) {
			t.Fatalf("bad: %v", services.NodeServices.Node)
		}
	}
}

func TestAgent_ServiceTokens(t *testing.T) {
	t.Parallel()

	tokens := new(token.Store)
	tokens.UpdateUserToken("default")
	cfg := config.DefaultRuntimeConfig(`bind_addr = "127.0.0.1" data_dir = "dummy"`)
	l := local.NewState(agent.LocalConfig(cfg), nil, tokens)
	l.TriggerSyncChanges = func() {}

	l.AddService(&structs.NodeService{ID: "redis"}, "")

	if token := l.ServiceToken("redis"); token != "default" {
		t.Fatalf("bad: %s", token)
	}

	l.AddService(&structs.NodeService{ID: "redis"}, "abc123")
	if token := l.ServiceToken("redis"); token != "abc123" {
		t.Fatalf("bad: %s", token)
	}

	l.RemoveService("redis")
	if token := l.ServiceToken("redis"); token != "abc123" {
		t.Fatalf("bad: %s", token)
	}
}

func TestAgent_CheckTokens(t *testing.T) {
	t.Parallel()

	tokens := new(token.Store)
	tokens.UpdateUserToken("default")
	cfg := config.DefaultRuntimeConfig(`bind_addr = "127.0.0.1" data_dir = "dummy"`)
	l := local.NewState(agent.LocalConfig(cfg), nil, tokens)
	l.TriggerSyncChanges = func() {}

	l.AddCheck(&structs.HealthCheck{CheckID: types.CheckID("mem")}, "")
	if token := l.CheckToken("mem"); token != "default" {
		t.Fatalf("bad: %s", token)
	}

	l.AddCheck(&structs.HealthCheck{CheckID: types.CheckID("mem")}, "abc123")
	if token := l.CheckToken("mem"); token != "abc123" {
		t.Fatalf("bad: %s", token)
	}

	l.RemoveCheck("mem")
	if token := l.CheckToken("mem"); token != "abc123" {
		t.Fatalf("bad: %s", token)
	}
}

func TestAgent_CheckCriticalTime(t *testing.T) {
	t.Parallel()
	cfg := config.DefaultRuntimeConfig(`bind_addr = "127.0.0.1" data_dir = "dummy"`)
	l := local.NewState(agent.LocalConfig(cfg), nil, new(token.Store))
	l.TriggerSyncChanges = func() {}

	svc := &structs.NodeService{ID: "redis", Service: "redis", Port: 8000}
	l.AddService(svc, "")

	checkID := types.CheckID("redis:1")
	chk := &structs.HealthCheck{
		Node:      "node",
		CheckID:   checkID,
		Name:      "redis:1",
		ServiceID: "redis",
		Status:    api.HealthPassing,
	}
	l.AddCheck(chk, "")
	if checks := l.CriticalCheckStates(); len(checks) > 0 {
		t.Fatalf("should not have any critical checks")
	}

	l.UpdateCheck(checkID, api.HealthWarning, "")
	if checks := l.CriticalCheckStates(); len(checks) > 0 {
		t.Fatalf("should not have any critical checks")
	}

	l.UpdateCheck(checkID, api.HealthCritical, "")
	if c, ok := l.CriticalCheckStates()[checkID]; !ok {
		t.Fatalf("should have a critical check")
	} else if c.CriticalFor() > time.Millisecond {
		t.Fatalf("bad: %#v", c)
	}

	time.Sleep(50 * time.Millisecond)
	l.UpdateCheck(chk.CheckID, api.HealthCritical, "")
	if c, ok := l.CriticalCheckStates()[checkID]; !ok {
		t.Fatalf("should have a critical check")
	} else if c.CriticalFor() < 25*time.Millisecond ||
		c.CriticalFor() > 75*time.Millisecond {
		t.Fatalf("bad: %#v", c)
	}

	l.UpdateCheck(checkID, api.HealthPassing, "")
	if checks := l.CriticalCheckStates(); len(checks) > 0 {
		t.Fatalf("should not have any critical checks")
	}

	l.UpdateCheck(checkID, api.HealthCritical, "")
	if c, ok := l.CriticalCheckStates()[checkID]; !ok {
		t.Fatalf("should have a critical check")
	} else if c.CriticalFor() > time.Millisecond {
		t.Fatalf("bad: %#v", c)
	}
}

func TestAgent_AddCheckFailure(t *testing.T) {
	t.Parallel()
	cfg := config.DefaultRuntimeConfig(`bind_addr = "127.0.0.1" data_dir = "dummy"`)
	l := local.NewState(agent.LocalConfig(cfg), nil, new(token.Store))
	l.TriggerSyncChanges = func() {}

	checkID := types.CheckID("redis:1")
	chk := &structs.HealthCheck{
		Node:      "node",
		CheckID:   checkID,
		Name:      "redis:1",
		ServiceID: "redis",
		Status:    api.HealthPassing,
	}
	wantErr := errors.New(`Check "redis:1" refers to non-existent service "redis"`)
	if got, want := l.AddCheck(chk, ""), wantErr; !reflect.DeepEqual(got, want) {
		t.Fatalf("got error %q want %q", got, want)
	}
}

func TestAgent_sendCoordinate(t *testing.T) {
	t.Parallel()
	a := agent.NewTestAgent(t.Name(), `
		sync_coordinate_interval_min = "1ms"
		sync_coordinate_rate_target = 10.0
		consul = {
			coordinate = {
				update_period = "100ms"
				update_batch_size = 10
				update_max_batches = 1
			}
		}
	`)
	defer a.Shutdown()

	t.Logf("%d %d %s",
		a.Config.ConsulCoordinateUpdateBatchSize,
		a.Config.ConsulCoordinateUpdateMaxBatches,
		a.Config.ConsulCoordinateUpdatePeriod.String())

	req := structs.DCSpecificRequest{
		Datacenter: a.Config.Datacenter,
	}
	var reply structs.IndexedCoordinates
	retry.Run(t, func(r *retry.R) {
		if err := a.RPC("Coordinate.ListNodes", &req, &reply); err != nil {
			r.Fatalf("err: %s", err)
		}
		if len(reply.Coordinates) != 1 {
			r.Fatalf("expected a coordinate: %v", reply)
		}
		coord := reply.Coordinates[0]
		if coord.Node != a.Config.NodeName || coord.Coord == nil {
			r.Fatalf("bad: %v", coord)
		}
	})
}

func servicesInSync(state *local.State, wantServices int) error {
	services := state.ServiceStates()
	if got, want := len(services), wantServices; got != want {
		return fmt.Errorf("got %d services want %d", got, want)
	}
	for id, s := range services {
		if !s.InSync {
			return fmt.Errorf("service %q should be in sync", id)
		}
	}
	return nil
}

func checksInSync(state *local.State, wantChecks int) error {
	checks := state.CheckStates()
	if got, want := len(checks), wantChecks; got != want {
		return fmt.Errorf("got %d checks want %d", got, want)
	}
	for id, c := range checks {
		if !c.InSync {
			return fmt.Errorf("check %q should be in sync", id)
		}
	}
	return nil
}
