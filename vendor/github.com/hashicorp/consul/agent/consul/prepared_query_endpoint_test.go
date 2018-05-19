package consul

import (
	"bytes"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/testrpc"
	"github.com/hashicorp/consul/testutil/retry"
	"github.com/hashicorp/consul/types"
	"github.com/hashicorp/net-rpc-msgpackrpc"
	"github.com/hashicorp/serf/coordinate"
)

func TestPreparedQuery_Apply(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	query := structs.PreparedQueryRequest{
		Datacenter: "dc1",
		Op:         structs.PreparedQueryCreate,
		Query: &structs.PreparedQuery{
			Name: "test",
			Service: structs.ServiceQuery{
				Service: "redis",
			},
		},
	}
	var reply string

	query.Query.ID = "nope"
	err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply)
	if err == nil || !strings.Contains(err.Error(), "ID must be empty") {
		t.Fatalf("bad: %v", err)
	}

	query.Op = structs.PreparedQueryUpdate
	query.Query.ID = generateUUID()
	err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply)
	if err == nil || !strings.Contains(err.Error(), "Cannot modify non-existent prepared query") {
		t.Fatalf("bad: %v", err)
	}

	query.Op = structs.PreparedQueryCreate
	query.Query.ID = ""
	query.Query.Service.Failover.NearestN = -1
	err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply)
	if err == nil || !strings.Contains(err.Error(), "Bad NearestN") {
		t.Fatalf("bad: %v", err)
	}

	query.Query.Service.Failover.NearestN = 0
	query.Query.Session = "nope"
	err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply)
	if err == nil || !strings.Contains(err.Error(), "failed session lookup") {
		t.Fatalf("bad: %v", err)
	}

	query.Query.Session = ""
	if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	query.Query.ID = reply
	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter: "dc1",
			QueryID:    query.Query.ID,
		}
		var resp structs.IndexedPreparedQueries
		if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	query.Op = structs.PreparedQueryUpdate
	query.Query.Service.Failover.NearestN = 2
	if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter: "dc1",
			QueryID:    query.Query.ID,
		}
		var resp structs.IndexedPreparedQueries
		if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	query.Op = "nope"
	err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply)
	if err == nil || !strings.Contains(err.Error(), "Unknown prepared query operation:") {
		t.Fatalf("bad: %v", err)
	}

	query.Op = structs.PreparedQueryUpdate
	query.Query.Service.Failover.NearestN = -1
	err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply)
	if err == nil || !strings.Contains(err.Error(), "Bad NearestN") {
		t.Fatalf("bad: %v", err)
	}

	query.Op = structs.PreparedQueryDelete
	if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter: "dc1",
			QueryID:    query.Query.ID,
		}
		var resp structs.IndexedPreparedQueries
		if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			if err.Error() != ErrQueryNotFound.Error() {
				t.Fatalf("err: %v", err)
			}
		}

		if len(resp.Queries) != 0 {
			t.Fatalf("bad: %v", resp)
		}
	}
}

func TestPreparedQuery_Apply_ACLDeny(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	var token string
	{
		var rules = `
                    query "redis" {
                        policy = "write"
                    }
                `

		req := structs.ACLRequest{
			Datacenter: "dc1",
			Op:         structs.ACLSet,
			ACL: structs.ACL{
				Name:  "User token",
				Type:  structs.ACLTypeClient,
				Rules: rules,
			},
			WriteRequest: structs.WriteRequest{Token: "root"},
		}
		if err := msgpackrpc.CallWithCodec(codec, "ACL.Apply", &req, &token); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	query := structs.PreparedQueryRequest{
		Datacenter: "dc1",
		Op:         structs.PreparedQueryCreate,
		Query: &structs.PreparedQuery{
			Name: "redis-master",
			Service: structs.ServiceQuery{
				Service: "the-redis",
			},
		},
	}
	var reply string

	err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}

	query.WriteRequest.Token = token
	if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	query.Query.ID = reply
	query.Query.Token = ""
	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      query.Query.ID,
			QueryOptions: structs.QueryOptions{Token: "root"},
		}
		var resp structs.IndexedPreparedQueries
		if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	query.Op = structs.PreparedQueryUpdate
	query.WriteRequest.Token = ""
	err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}

	query.WriteRequest.Token = token
	if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	query.Op = structs.PreparedQueryDelete
	query.WriteRequest.Token = ""
	err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}

	query.WriteRequest.Token = token
	if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      query.Query.ID,
			QueryOptions: structs.QueryOptions{Token: "root"},
		}
		var resp structs.IndexedPreparedQueries
		if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			if err.Error() != ErrQueryNotFound.Error() {
				t.Fatalf("err: %v", err)
			}
		}

		if len(resp.Queries) != 0 {
			t.Fatalf("bad: %v", resp)
		}
	}

	query.Op = structs.PreparedQueryCreate
	query.Query.ID = ""
	query.WriteRequest.Token = token
	if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	query.Query.ID = reply
	query.Query.Token = ""
	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      query.Query.ID,
			QueryOptions: structs.QueryOptions{Token: "root"},
		}
		var resp structs.IndexedPreparedQueries
		if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	query.Op = structs.PreparedQueryUpdate
	query.WriteRequest.Token = "root"
	if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	query.Query.Token = ""
	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      query.Query.ID,
			QueryOptions: structs.QueryOptions{Token: "root"},
		}
		var resp structs.IndexedPreparedQueries
		if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	query.Op = structs.PreparedQueryDelete
	query.WriteRequest.Token = "root"
	if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      query.Query.ID,
			QueryOptions: structs.QueryOptions{Token: "root"},
		}
		var resp structs.IndexedPreparedQueries
		if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			if err.Error() != ErrQueryNotFound.Error() {
				t.Fatalf("err: %v", err)
			}
		}

		if len(resp.Queries) != 0 {
			t.Fatalf("bad: %v", resp)
		}
	}

	query.Op = structs.PreparedQueryCreate
	query.Query.ID = ""
	query.Query.Name = "cassandra"
	query.WriteRequest.Token = "root"
	if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	query.Query.ID = reply
	query.Query.Token = ""
	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      query.Query.ID,
			QueryOptions: structs.QueryOptions{Token: "root"},
		}
		var resp structs.IndexedPreparedQueries
		if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	query.Op = structs.PreparedQueryUpdate
	query.Query.Name = "redis"
	query.WriteRequest.Token = token
	err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}
}

func TestPreparedQuery_Apply_ForwardLeader(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.Bootstrap = false
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec1 := rpcClient(t, s1)
	defer codec1.Close()

	dir2, s2 := testServer(t)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()
	codec2 := rpcClient(t, s2)
	defer codec2.Close()

	joinLAN(t, s2, s1)

	testrpc.WaitForLeader(t, s1.RPC, "dc1")
	testrpc.WaitForLeader(t, s2.RPC, "dc1")

	var codec rpc.ClientCodec
	if !s1.IsLeader() {
		codec = codec1
	} else {
		codec = codec2
	}

	{
		req := structs.RegisterRequest{
			Datacenter: "dc1",
			Node:       "foo",
			Address:    "127.0.0.1",
			Service: &structs.NodeService{
				Service: "redis",
				Tags:    []string{"master"},
				Port:    8000,
			},
		}
		var reply struct{}
		err := msgpackrpc.CallWithCodec(codec, "Catalog.Register", &req, &reply)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	query := structs.PreparedQueryRequest{
		Datacenter: "dc1",
		Op:         structs.PreparedQueryCreate,
		Query: &structs.PreparedQuery{
			Name: "test",
			Service: structs.ServiceQuery{
				Service: "redis",
			},
		},
	}

	var reply string
	if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestPreparedQuery_parseQuery(t *testing.T) {
	t.Parallel()
	query := &structs.PreparedQuery{}

	err := parseQuery(query, true)
	if err == nil || !strings.Contains(err.Error(), "Must be bound to a session") {
		t.Fatalf("bad: %v", err)
	}

	query.Session = "adf4238a-882b-9ddc-4a9d-5b6758e4159e"
	err = parseQuery(query, true)
	if err == nil || !strings.Contains(err.Error(), "Must provide a Service") {
		t.Fatalf("bad: %v", err)
	}

	query.Session = ""
	query.Template.Type = "some-kind-of-template"
	err = parseQuery(query, true)
	if err == nil || !strings.Contains(err.Error(), "Must provide a Service") {
		t.Fatalf("bad: %v", err)
	}

	query.Template.Type = ""
	err = parseQuery(query, false)
	if err == nil || !strings.Contains(err.Error(), "Must provide a Service") {
		t.Fatalf("bad: %v", err)
	}

	for _, version8 := range []bool{true, false} {
		query = &structs.PreparedQuery{}
		query.Session = "adf4238a-882b-9ddc-4a9d-5b6758e4159e"
		query.Service.Service = "foo"
		if err := parseQuery(query, version8); err != nil {
			t.Fatalf("err: %v", err)
		}

		query.Token = redactedToken
		err = parseQuery(query, version8)
		if err == nil || !strings.Contains(err.Error(), "Bad Token") {
			t.Fatalf("bad: %v", err)
		}

		query.Token = "adf4238a-882b-9ddc-4a9d-5b6758e4159e"
		if err := parseQuery(query, version8); err != nil {
			t.Fatalf("err: %v", err)
		}

		query.Service.Failover.NearestN = -1
		err = parseQuery(query, version8)
		if err == nil || !strings.Contains(err.Error(), "Bad NearestN") {
			t.Fatalf("bad: %v", err)
		}

		query.Service.Failover.NearestN = 3
		if err := parseQuery(query, version8); err != nil {
			t.Fatalf("err: %v", err)
		}

		query.DNS.TTL = "two fortnights"
		err = parseQuery(query, version8)
		if err == nil || !strings.Contains(err.Error(), "Bad DNS TTL") {
			t.Fatalf("bad: %v", err)
		}

		query.DNS.TTL = "-3s"
		err = parseQuery(query, version8)
		if err == nil || !strings.Contains(err.Error(), "must be >=0") {
			t.Fatalf("bad: %v", err)
		}

		query.DNS.TTL = "3s"
		if err := parseQuery(query, version8); err != nil {
			t.Fatalf("err: %v", err)
		}

		query.Service.NodeMeta = map[string]string{"": "somevalue"}
		err = parseQuery(query, version8)
		if err == nil || !strings.Contains(err.Error(), "cannot be blank") {
			t.Fatalf("bad: %v", err)
		}

		query.Service.NodeMeta = map[string]string{"somekey": "somevalue"}
		if err := parseQuery(query, version8); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
}

func TestPreparedQuery_ACLDeny_Catchall_Template(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	var token string
	{
		var rules = `
                    query "" {
                        policy = "write"
                    }
                `

		req := structs.ACLRequest{
			Datacenter: "dc1",
			Op:         structs.ACLSet,
			ACL: structs.ACL{
				Name:  "User token",
				Type:  structs.ACLTypeClient,
				Rules: rules,
			},
			WriteRequest: structs.WriteRequest{Token: "root"},
		}
		if err := msgpackrpc.CallWithCodec(codec, "ACL.Apply", &req, &token); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	query := structs.PreparedQueryRequest{
		Datacenter: "dc1",
		Op:         structs.PreparedQueryCreate,
		Query: &structs.PreparedQuery{
			Name:  "",
			Token: "5e1e24e5-1329-f86f-18c6-3d3734edb2cd",
			Template: structs.QueryTemplateOptions{
				Type: structs.QueryTemplateTypeNamePrefixMatch,
			},
			Service: structs.ServiceQuery{
				Service: "${name.full}",
			},
		},
	}
	var reply string

	err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply)
	if !acl.IsErrPermissionDenied(err) {
		t.Fatalf("bad: %v", err)
	}

	query.WriteRequest.Token = token
	if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	query.Query.ID = reply
	query.Query.Token = redactedToken
	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      query.Query.ID,
			QueryOptions: structs.QueryOptions{Token: token},
		}
		var resp structs.IndexedPreparedQueries
		if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter: "dc1",
			QueryID:    query.Query.ID,
		}
		var resp structs.IndexedPreparedQueries
		err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp)
		if !acl.IsErrPermissionDenied(err) {
			t.Fatalf("bad: %v", err)
		}

		if len(resp.Queries) != 0 {
			t.Fatalf("bad: %v", resp)
		}
	}

	{
		req := &structs.DCSpecificRequest{
			Datacenter: "dc1",
		}
		var resp structs.IndexedPreparedQueries
		if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.List", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 0 {
			t.Fatalf("bad: %v", resp)
		}
	}

	query.Query.Token = "5e1e24e5-1329-f86f-18c6-3d3734edb2cd"
	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      query.Query.ID,
			QueryOptions: structs.QueryOptions{Token: "root"},
		}
		var resp structs.IndexedPreparedQueries
		if err = msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	{
		req := &structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: "anything",
		}
		var resp structs.PreparedQueryExplainResponse
		err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Explain", req, &resp)
		if !acl.IsErrPermissionDenied(err) {
			t.Fatalf("bad: %v", err)
		}
	}

	query.Query.Token = redactedToken
	query.Query.Service.Service = "anything"
	{
		req := &structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: "anything",
			QueryOptions:  structs.QueryOptions{Token: token},
		}
		var resp structs.PreparedQueryExplainResponse
		err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Explain", req, &resp)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		actual := &resp.Query
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	query.Query.Token = "5e1e24e5-1329-f86f-18c6-3d3734edb2cd"
	query.Query.Service.Service = "anything"
	{
		req := &structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: "anything",
			QueryOptions:  structs.QueryOptions{Token: "root"},
		}
		var resp structs.PreparedQueryExplainResponse
		err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Explain", req, &resp)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		actual := &resp.Query
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}
}

func TestPreparedQuery_Get(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	var token string
	{
		var rules = `
                    query "redis" {
                        policy = "write"
                    }
                `

		req := structs.ACLRequest{
			Datacenter: "dc1",
			Op:         structs.ACLSet,
			ACL: structs.ACL{
				Name:  "User token",
				Type:  structs.ACLTypeClient,
				Rules: rules,
			},
			WriteRequest: structs.WriteRequest{Token: "root"},
		}
		if err := msgpackrpc.CallWithCodec(codec, "ACL.Apply", &req, &token); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	query := structs.PreparedQueryRequest{
		Datacenter: "dc1",
		Op:         structs.PreparedQueryCreate,
		Query: &structs.PreparedQuery{
			Name: "redis-master",
			Service: structs.ServiceQuery{
				Service: "the-redis",
			},
		},
		WriteRequest: structs.WriteRequest{Token: token},
	}
	var reply string
	if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	query.Query.ID = reply
	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      query.Query.ID,
			QueryOptions: structs.QueryOptions{Token: token},
		}
		var resp structs.IndexedPreparedQueries
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      query.Query.ID,
			QueryOptions: structs.QueryOptions{Token: ""},
		}
		var resp structs.IndexedPreparedQueries
		err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp)
		if !acl.IsErrPermissionDenied(err) {
			t.Fatalf("bad: %v", err)
		}

		if len(resp.Queries) != 0 {
			t.Fatalf("bad: %v", resp)
		}
	}

	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      query.Query.ID,
			QueryOptions: structs.QueryOptions{Token: "root"},
		}
		var resp structs.IndexedPreparedQueries
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	var session string
	{
		req := structs.SessionRequest{
			Datacenter: "dc1",
			Op:         structs.SessionCreate,
			Session: structs.Session{
				Node: s1.config.NodeName,
			},
		}
		if err := msgpackrpc.CallWithCodec(codec, "Session.Apply", &req, &session); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	query.Op = structs.PreparedQueryUpdate
	query.Query.Name = ""
	query.Query.Session = session
	if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      query.Query.ID,
			QueryOptions: structs.QueryOptions{Token: ""},
		}
		var resp structs.IndexedPreparedQueries
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	query.Op = structs.PreparedQueryUpdate
	query.Query.Token = "le-token"
	if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	query.Query.Token = redactedToken
	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      query.Query.ID,
			QueryOptions: structs.QueryOptions{Token: ""},
		}
		var resp structs.IndexedPreparedQueries
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	query.Query.Token = "le-token"
	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      query.Query.ID,
			QueryOptions: structs.QueryOptions{Token: "root"},
		}
		var resp structs.IndexedPreparedQueries
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	{
		req := &structs.PreparedQuerySpecificRequest{
			Datacenter:   "dc1",
			QueryID:      generateUUID(),
			QueryOptions: structs.QueryOptions{Token: token},
		}
		var resp structs.IndexedPreparedQueries
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Get", req, &resp); err != nil {
			if err.Error() != ErrQueryNotFound.Error() {
				t.Fatalf("err: %v", err)
			}
		}

		if len(resp.Queries) != 0 {
			t.Fatalf("bad: %v", resp)
		}
	}
}

func TestPreparedQuery_List(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	var token string
	{
		var rules = `
                    query "redis" {
                        policy = "write"
                    }
                `

		req := structs.ACLRequest{
			Datacenter: "dc1",
			Op:         structs.ACLSet,
			ACL: structs.ACL{
				Name:  "User token",
				Type:  structs.ACLTypeClient,
				Rules: rules,
			},
			WriteRequest: structs.WriteRequest{Token: "root"},
		}
		if err := msgpackrpc.CallWithCodec(codec, "ACL.Apply", &req, &token); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	{
		req := &structs.DCSpecificRequest{
			Datacenter:   "dc1",
			QueryOptions: structs.QueryOptions{Token: token},
		}
		var resp structs.IndexedPreparedQueries
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.List", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 0 {
			t.Fatalf("bad: %v", resp)
		}
	}

	query := structs.PreparedQueryRequest{
		Datacenter: "dc1",
		Op:         structs.PreparedQueryCreate,
		Query: &structs.PreparedQuery{
			Name:  "redis-master",
			Token: "le-token",
			Service: structs.ServiceQuery{
				Service: "the-redis",
			},
		},
		WriteRequest: structs.WriteRequest{Token: token},
	}
	var reply string
	if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	query.Query.ID = reply
	query.Query.Token = redactedToken
	{
		req := &structs.DCSpecificRequest{
			Datacenter:   "dc1",
			QueryOptions: structs.QueryOptions{Token: token},
		}
		var resp structs.IndexedPreparedQueries
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.List", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	{
		req := &structs.DCSpecificRequest{
			Datacenter:   "dc1",
			QueryOptions: structs.QueryOptions{Token: ""},
		}
		var resp structs.IndexedPreparedQueries
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.List", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 0 {
			t.Fatalf("bad: %v", resp)
		}
	}

	query.Query.Token = "le-token"
	{
		req := &structs.DCSpecificRequest{
			Datacenter:   "dc1",
			QueryOptions: structs.QueryOptions{Token: "root"},
		}
		var resp structs.IndexedPreparedQueries
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.List", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	var session string
	{
		req := structs.SessionRequest{
			Datacenter: "dc1",
			Op:         structs.SessionCreate,
			Session: structs.Session{
				Node: s1.config.NodeName,
			},
		}
		if err := msgpackrpc.CallWithCodec(codec, "Session.Apply", &req, &session); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	query.Op = structs.PreparedQueryUpdate
	query.Query.Name = ""
	query.Query.Session = session
	if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := &structs.DCSpecificRequest{
			Datacenter:   "dc1",
			QueryOptions: structs.QueryOptions{Token: token},
		}
		var resp structs.IndexedPreparedQueries
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.List", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 0 {
			t.Fatalf("bad: %v", resp)
		}
	}

	{
		req := &structs.DCSpecificRequest{
			Datacenter:   "dc1",
			QueryOptions: structs.QueryOptions{Token: "root"},
		}
		var resp structs.IndexedPreparedQueries
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.List", req, &resp); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(resp.Queries) != 1 {
			t.Fatalf("bad: %v", resp)
		}
		actual := resp.Queries[0]
		if resp.Index != actual.ModifyIndex {
			t.Fatalf("bad index: %d", resp.Index)
		}
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}
}

func TestPreparedQuery_Explain(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec := rpcClient(t, s1)
	defer codec.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")

	var token string
	{
		var rules = `
                    query "prod-" {
                        policy = "write"
                    }
                `

		req := structs.ACLRequest{
			Datacenter: "dc1",
			Op:         structs.ACLSet,
			ACL: structs.ACL{
				Name:  "User token",
				Type:  structs.ACLTypeClient,
				Rules: rules,
			},
			WriteRequest: structs.WriteRequest{Token: "root"},
		}
		if err := msgpackrpc.CallWithCodec(codec, "ACL.Apply", &req, &token); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	query := structs.PreparedQueryRequest{
		Datacenter: "dc1",
		Op:         structs.PreparedQueryCreate,
		Query: &structs.PreparedQuery{
			Name:  "prod-",
			Token: "5e1e24e5-1329-f86f-18c6-3d3734edb2cd",
			Template: structs.QueryTemplateOptions{
				Type: structs.QueryTemplateTypeNamePrefixMatch,
			},
			Service: structs.ServiceQuery{
				Service: "${name.full}",
			},
		},
		WriteRequest: structs.WriteRequest{Token: token},
	}
	var reply string
	if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	query.Query.ID = reply
	query.Query.Service.Service = "prod-redis"
	{
		req := &structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: "prod-redis",
			QueryOptions:  structs.QueryOptions{Token: "root"},
		}
		var resp structs.PreparedQueryExplainResponse
		err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Explain", req, &resp)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		actual := &resp.Query
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	query.Query.Token = redactedToken
	query.Query.Service.Service = "prod-redis"
	{
		req := &structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: "prod-redis",
			QueryOptions:  structs.QueryOptions{Token: token},
		}
		var resp structs.PreparedQueryExplainResponse
		err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Explain", req, &resp)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		actual := &resp.Query
		actual.CreateIndex, actual.ModifyIndex = 0, 0
		if !reflect.DeepEqual(actual, query.Query) {
			t.Fatalf("bad: %v", actual)
		}
	}

	{
		req := &structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: "prod-redis",
		}
		var resp structs.PreparedQueryExplainResponse
		err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Explain", req, &resp)
		if !acl.IsErrPermissionDenied(err) {
			t.Fatalf("bad: %v", err)
		}
	}

	{
		req := &structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: generateUUID(),
			QueryOptions:  structs.QueryOptions{Token: "root"},
		}
		var resp structs.IndexedPreparedQueries
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Explain", req, &resp); err != nil {
			if err.Error() != ErrQueryNotFound.Error() {
				t.Fatalf("err: %v", err)
			}
		}
	}
}

func TestPreparedQuery_Execute(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
		c.ACLEnforceVersion8 = false
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec1 := rpcClient(t, s1)
	defer codec1.Close()

	dir2, s2 := testServerWithConfig(t, func(c *Config) {
		c.Datacenter = "dc2"
		c.ACLDatacenter = "dc1"
	})
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()
	codec2 := rpcClient(t, s2)
	defer codec2.Close()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")
	testrpc.WaitForLeader(t, s2.RPC, "dc2")

	joinWAN(t, s2, s1)
	retry.Run(t, func(r *retry.R) {
		if got, want := len(s1.WANMembers()), 2; got != want {
			r.Fatalf("got %d WAN members want %d", got, want)
		}
	})

	var execToken string
	{
		var rules = `
                    service "foo" {
                        policy = "read"
                    }
                `

		req := structs.ACLRequest{
			Datacenter: "dc1",
			Op:         structs.ACLSet,
			ACL: structs.ACL{
				Name:  "User token",
				Type:  structs.ACLTypeClient,
				Rules: rules,
			},
			WriteRequest: structs.WriteRequest{Token: "root"},
		}
		if err := msgpackrpc.CallWithCodec(codec1, "ACL.Apply", &req, &execToken); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	{
		for i := 0; i < 10; i++ {
			for _, dc := range []string{"dc1", "dc2"} {
				req := structs.RegisterRequest{
					Datacenter: dc,
					Node:       fmt.Sprintf("node%d", i+1),
					Address:    fmt.Sprintf("127.0.0.%d", i+1),
					NodeMeta: map[string]string{
						"group":         fmt.Sprintf("%d", i/5),
						"instance_type": "t2.micro",
					},
					Service: &structs.NodeService{
						Service: "foo",
						Port:    8000,
						Tags:    []string{dc, fmt.Sprintf("tag%d", i+1)},
					},
					WriteRequest: structs.WriteRequest{Token: "root"},
				}
				if i == 0 {
					req.NodeMeta["unique"] = "true"
				}

				var codec rpc.ClientCodec
				if dc == "dc1" {
					codec = codec1
				} else {
					codec = codec2
				}

				var reply struct{}
				if err := msgpackrpc.CallWithCodec(codec, "Catalog.Register", &req, &reply); err != nil {
					t.Fatalf("err: %v", err)
				}
			}
		}
	}

	query := structs.PreparedQueryRequest{
		Datacenter: "dc1",
		Op:         structs.PreparedQueryCreate,
		Query: &structs.PreparedQuery{
			Name: "test",
			Service: structs.ServiceQuery{
				Service: "foo",
			},
			DNS: structs.QueryDNSOptions{
				TTL: "10s",
			},
		},
		WriteRequest: structs.WriteRequest{Token: "root"},
	}
	if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Apply", &query, &query.Query.ID); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: "nope",
		}

		var reply structs.PreparedQueryExecuteResponse
		err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply)
		if err == nil || err.Error() != ErrQueryNotFound.Error() {
			t.Fatalf("bad: %v", err)
		}

		if len(reply.Nodes) != 0 {
			t.Fatalf("bad: %v", reply)
		}
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 10 ||
			reply.Datacenter != "dc1" || reply.Failovers != 0 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			Limit:         3,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 3 ||
			reply.Datacenter != "dc1" || reply.Failovers != 0 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
	}

	if false {
		cases := []struct {
			filters  map[string]string
			numNodes int
		}{
			{
				filters:  map[string]string{},
				numNodes: 10,
			},
			{
				filters:  map[string]string{"instance_type": "t2.micro"},
				numNodes: 10,
			},
			{
				filters:  map[string]string{"group": "1"},
				numNodes: 5,
			},
			{
				filters:  map[string]string{"group": "0", "unique": "true"},
				numNodes: 1,
			},
		}

		for _, tc := range cases {
			nodeMetaQuery := structs.PreparedQueryRequest{
				Datacenter: "dc1",
				Op:         structs.PreparedQueryCreate,
				Query: &structs.PreparedQuery{
					Service: structs.ServiceQuery{
						Service:  "foo",
						NodeMeta: tc.filters,
					},
					DNS: structs.QueryDNSOptions{
						TTL: "10s",
					},
				},
				WriteRequest: structs.WriteRequest{Token: "root"},
			}
			if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Apply", &nodeMetaQuery, &nodeMetaQuery.Query.ID); err != nil {
				t.Fatalf("err: %v", err)
			}

			req := structs.PreparedQueryExecuteRequest{
				Datacenter:    "dc1",
				QueryIDOrName: nodeMetaQuery.Query.ID,
				QueryOptions:  structs.QueryOptions{Token: execToken},
			}

			var reply structs.PreparedQueryExecuteResponse
			if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
				t.Fatalf("err: %v", err)
			}

			if len(reply.Nodes) != tc.numNodes {
				t.Fatalf("bad: %v, %v", len(reply.Nodes), tc.numNodes)
			}

			for _, node := range reply.Nodes {
				if !structs.SatisfiesMetaFilters(node.Node.Meta, tc.filters) {
					t.Fatalf("bad: %v", node.Node.Meta)
				}
			}
		}
	}

	{
		req := structs.CoordinateUpdateRequest{
			Datacenter: "dc1",
			Node:       "node3",
			Coord:      coordinate.NewCoordinate(coordinate.DefaultConfig()),
		}
		var out struct{}
		if err := msgpackrpc.CallWithCodec(codec1, "Coordinate.Update", &req, &out); err != nil {
			t.Fatalf("err: %v", err)
		}
		time.Sleep(3 * s1.config.CoordinateUpdatePeriod)
	}

	for i := 0; i < 100; i++ {
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			Source: structs.QuerySource{
				Datacenter: "dc1",
				Node:       "node3",
			},
			QueryOptions: structs.QueryOptions{Token: execToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 10 ||
			reply.Datacenter != "dc1" || reply.Failovers != 0 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
		if reply.Nodes[0].Node.Node != "node3" {
			t.Fatalf("bad: %v", reply)
		}
	}

	uniques := make(map[string]struct{})
	for i := 0; i < 100; i++ {
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 10 ||
			reply.Datacenter != "dc1" || reply.Failovers != 0 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
		var names []string
		for _, node := range reply.Nodes {
			names = append(names, node.Node.Node)
		}
		key := strings.Join(names, "|")
		uniques[key] = struct{}{}
	}

	if len(uniques) < 50 {
		t.Fatalf("unique shuffle ratio too low: %d/100", len(uniques))
	}

	query.Op = structs.PreparedQueryUpdate
	query.Query.Service.Near = "node3"
	if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Apply", &query, &query.Query.ID); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Agent: structs.QuerySource{
				Datacenter: "dc1",
				Node:       "node3",
			},
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		for i := 0; i < 10; i++ {
			var reply structs.PreparedQueryExecuteResponse
			if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
				t.Fatalf("err: %v", err)
			}
			if n := len(reply.Nodes); n != 10 {
				t.Fatalf("expect 10 nodes, got: %d", n)
			}
			if node := reply.Nodes[0].Node.Node; node != "node3" {
				t.Fatalf("expect node3 first, got: %q", node)
			}
		}
	}

	{

		req := structs.PreparedQueryExecuteRequest{
			Source: structs.QuerySource{
				Datacenter: "dc1",
				Node:       "foo",
			},
			Agent: structs.QuerySource{
				Datacenter: "dc1",
				Node:       "node3",
			},
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		shuffled := false
		for i := 0; i < 10; i++ {
			var reply structs.PreparedQueryExecuteResponse
			if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
				t.Fatalf("err: %v", err)
			}
			if n := len(reply.Nodes); n != 10 {
				t.Fatalf("expect 10 nodes, got: %d", n)
			}
			if node := reply.Nodes[0].Node.Node; node != "node3" {
				shuffled = true
				break
			}
		}

		if !shuffled {
			t.Fatalf("expect nodes to be shuffled")
		}
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Source: structs.QuerySource{
				Datacenter: "dc1",
				Node:       "node1",
			},
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		for i := 0; i < 10; i++ {
			var reply structs.PreparedQueryExecuteResponse
			if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
				t.Fatalf("err: %v", err)
			}
			if n := len(reply.Nodes); n != 10 {
				t.Fatalf("expect 10 nodes, got: %d", n)
			}
			if node := reply.Nodes[0].Node.Node; node != "node1" {
				t.Fatalf("expect node1 first, got: %q", node)
			}
		}
	}

	query.Query.Service.Near = "_agent"
	if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Apply", &query, &query.Query.ID); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Agent: structs.QuerySource{
				Datacenter: "dc1",
				Node:       "node3",
			},
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		for i := 0; i < 10; i++ {
			var reply structs.PreparedQueryExecuteResponse
			if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
				t.Fatalf("err: %v", err)
			}
			if n := len(reply.Nodes); n != 10 {
				t.Fatalf("expect 10 nodes, got: %d", n)
			}
			if node := reply.Nodes[0].Node.Node; node != "node3" {
				t.Fatalf("expect node3 first, got: %q", node)
			}
		}
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Agent: structs.QuerySource{
				Datacenter: "dc1",
				Node:       "foo",
			},
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		shuffled := false
		for i := 0; i < 10; i++ {
			var reply structs.PreparedQueryExecuteResponse
			if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
				t.Fatalf("err: %v", err)
			}
			if n := len(reply.Nodes); n != 10 {
				t.Fatalf("expect 10 nodes, got: %d", n)
			}
			if node := reply.Nodes[0].Node.Node; node != "node3" {
				shuffled = true
				break
			}
		}

		if !shuffled {
			t.Fatal("expect nodes to be shuffled")
		}
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Source: structs.QuerySource{
				Datacenter: "dc2",
				Node:       "node3",
			},
			Agent: structs.QuerySource{
				Datacenter: "dc1",
				Node:       "node3",
			},
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		shuffled := false
		for i := 0; i < 10; i++ {
			var reply structs.PreparedQueryExecuteResponse
			if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
				t.Fatalf("err: %v", err)
			}
			if n := len(reply.Nodes); n != 10 {
				t.Fatalf("expect 10 nodes, got: %d", n)
			}
			if reply.Nodes[0].Node.Node != "node3" {
				shuffled = true
				break
			}
		}

		if !shuffled {
			t.Fatal("expect node shuffle for remote results")
		}
	}

	query.Query.Service.Near = ""
	if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Apply", &query, &query.Query.ID); err != nil {
		t.Fatalf("err: %v", err)
	}

	setHealth := func(node string, health string) {
		req := structs.RegisterRequest{
			Datacenter: "dc1",
			Node:       node,
			Address:    "127.0.0.1",
			Service: &structs.NodeService{
				Service: "foo",
				Port:    8000,
				Tags:    []string{"dc1", "tag1"},
			},
			Check: &structs.HealthCheck{
				Name:      "failing",
				Status:    health,
				ServiceID: "foo",
			},
			WriteRequest: structs.WriteRequest{Token: "root"},
		}
		var reply struct{}
		if err := msgpackrpc.CallWithCodec(codec1, "Catalog.Register", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	setHealth("node1", api.HealthCritical)

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 9 ||
			reply.Datacenter != "dc1" || reply.Failovers != 0 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
		for _, node := range reply.Nodes {
			if node.Node.Node == "node1" {
				t.Fatalf("bad: %v", node)
			}
		}
	}

	setHealth("node1", api.HealthWarning)
	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 10 ||
			reply.Datacenter != "dc1" || reply.Failovers != 0 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
	}

	query.Query.Service.OnlyPassing = true
	if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Apply", &query, &query.Query.ID); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 9 ||
			reply.Datacenter != "dc1" || reply.Failovers != 0 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
		for _, node := range reply.Nodes {
			if node.Node.Node == "node1" {
				t.Fatalf("bad: %v", node)
			}
		}
	}

	query.Query.Service.IgnoreCheckIDs = []types.CheckID{"failing"}
	if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Apply", &query, &query.Query.ID); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 10 ||
			reply.Datacenter != "dc1" ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
	}

	query.Query.Service.IgnoreCheckIDs = nil
	if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Apply", &query, &query.Query.ID); err != nil {
		t.Fatalf("err: %v", err)
	}

	query.Query.Service.Tags = []string{"!tag3"}
	if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Apply", &query, &query.Query.ID); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 8 ||
			reply.Datacenter != "dc1" || reply.Failovers != 0 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
		for _, node := range reply.Nodes {
			if node.Node.Node == "node1" || node.Node.Node == "node3" {
				t.Fatalf("bad: %v", node)
			}
		}
	}

	var denyToken string
	{
		var rules = `
                    service "foo" {
                        policy = "deny"
                    }
                `

		req := structs.ACLRequest{
			Datacenter: "dc1",
			Op:         structs.ACLSet,
			ACL: structs.ACL{
				Name:  "User token",
				Type:  structs.ACLTypeClient,
				Rules: rules,
			},
			WriteRequest: structs.WriteRequest{Token: "root"},
		}
		if err := msgpackrpc.CallWithCodec(codec1, "ACL.Apply", &req, &denyToken); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: denyToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 0 ||
			reply.Datacenter != "dc1" || reply.Failovers != 0 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
	}

	query.Query.Token = execToken
	if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Apply", &query, &query.Query.ID); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: denyToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 8 ||
			reply.Datacenter != "dc1" || reply.Failovers != 0 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
		for _, node := range reply.Nodes {
			if node.Node.Node == "node1" || node.Node.Node == "node3" {
				t.Fatalf("bad: %v", node)
			}
		}
	}

	query.Query.Token = ""
	if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Apply", &query, &query.Query.ID); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: denyToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 0 ||
			reply.Datacenter != "dc1" || reply.Failovers != 0 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
	}

	s1.config.ACLEnforceVersion8 = true
	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 0 ||
			reply.Datacenter != "dc1" || reply.Failovers != 0 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
	}

	s1.config.ACLEnforceVersion8 = false
	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 8 ||
			reply.Datacenter != "dc1" || reply.Failovers != 0 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
		for _, node := range reply.Nodes {
			if node.Node.Node == "node1" || node.Node.Node == "node3" {
				t.Fatalf("bad: %v", node)
			}
		}
	}

	for i := 0; i < 10; i++ {
		setHealth(fmt.Sprintf("node%d", i+1), api.HealthCritical)
	}
	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 0 ||
			reply.Datacenter != "dc1" || reply.Failovers != 0 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
	}

	query.Query.Service.Failover.Datacenters = []string{"bogus", "dc2"}
	if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Apply", &query, &query.Query.ID); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 9 ||
			reply.Datacenter != "dc2" || reply.Failovers != 1 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
		for _, node := range reply.Nodes {
			if node.Node.Node == "node3" {
				t.Fatalf("bad: %v", node)
			}
		}
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			Limit:         3,
			QueryOptions: structs.QueryOptions{
				Token:             execToken,
				RequireConsistent: true,
			},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 3 ||
			reply.Datacenter != "dc2" || reply.Failovers != 1 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
		for _, node := range reply.Nodes {
			if node.Node.Node == "node3" {
				t.Fatalf("bad: %v", node)
			}
		}
	}

	uniques = make(map[string]struct{})
	for i := 0; i < 100; i++ {
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: execToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 9 ||
			reply.Datacenter != "dc2" || reply.Failovers != 1 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
		var names []string
		for _, node := range reply.Nodes {
			names = append(names, node.Node.Node)
		}
		key := strings.Join(names, "|")
		uniques[key] = struct{}{}
	}

	if len(uniques) < 50 {
		t.Fatalf("unique shuffle ratio too low: %d/100", len(uniques))
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: denyToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 0 ||
			reply.Datacenter != "dc2" || reply.Failovers != 1 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
	}

	query.Query.Token = execToken
	if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Apply", &query, &query.Query.ID); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: query.Query.ID,
			QueryOptions:  structs.QueryOptions{Token: denyToken},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec1, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 9 ||
			reply.Datacenter != "dc2" || reply.Failovers != 1 ||
			reply.Service != query.Query.Service.Service ||
			!reflect.DeepEqual(reply.DNS, query.Query.DNS) ||
			!reply.QueryMeta.KnownLeader {
			t.Fatalf("bad: %v", reply)
		}
		for _, node := range reply.Nodes {
			if node.Node.Node == "node3" {
				t.Fatalf("bad: %v", node)
			}
		}
	}
}

func TestPreparedQuery_Execute_ForwardLeader(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServer(t)
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()
	codec1 := rpcClient(t, s1)
	defer codec1.Close()

	dir2, s2 := testServer(t)
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()
	codec2 := rpcClient(t, s2)
	defer codec2.Close()

	joinLAN(t, s2, s1)

	testrpc.WaitForLeader(t, s1.RPC, "dc1")
	testrpc.WaitForLeader(t, s2.RPC, "dc1")

	var codec rpc.ClientCodec
	if !s1.IsLeader() {
		codec = codec1
	} else {
		codec = codec2
	}

	{
		req := structs.RegisterRequest{
			Datacenter: "dc1",
			Node:       "foo",
			Address:    "127.0.0.1",
			Service: &structs.NodeService{
				Service: "redis",
				Tags:    []string{"master"},
				Port:    8000,
			},
		}
		var reply struct{}
		if err := msgpackrpc.CallWithCodec(codec, "Catalog.Register", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

	query := structs.PreparedQueryRequest{
		Datacenter: "dc1",
		Op:         structs.PreparedQueryCreate,
		Query: &structs.PreparedQuery{
			Name: "test",
			Service: structs.ServiceQuery{
				Service: "redis",
			},
		},
	}
	var reply string
	if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Apply", &query, &reply); err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: reply,
		}
		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 1 {
			t.Fatalf("bad: %v", reply)
		}
	}

	{
		req := structs.PreparedQueryExecuteRequest{
			Datacenter:    "dc1",
			QueryIDOrName: reply,
			QueryOptions:  structs.QueryOptions{RequireConsistent: true},
		}
		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.Execute", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 1 {
			t.Fatalf("bad: %v", reply)
		}
	}

	{
		req := structs.PreparedQueryExecuteRemoteRequest{
			Datacenter: "dc1",
			Query:      *query.Query,
		}
		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.ExecuteRemote", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 1 {
			t.Fatalf("bad: %v", reply)
		}
	}

	{
		req := structs.PreparedQueryExecuteRemoteRequest{
			Datacenter:   "dc1",
			Query:        *query.Query,
			QueryOptions: structs.QueryOptions{RequireConsistent: true},
		}
		var reply structs.PreparedQueryExecuteResponse
		if err := msgpackrpc.CallWithCodec(codec, "PreparedQuery.ExecuteRemote", &req, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}

		if len(reply.Nodes) != 1 {
			t.Fatalf("bad: %v", reply)
		}
	}
}

func TestPreparedQuery_tagFilter(t *testing.T) {
	t.Parallel()
	testNodes := func() structs.CheckServiceNodes {
		return structs.CheckServiceNodes{
			structs.CheckServiceNode{
				Node:    &structs.Node{Node: "node1"},
				Service: &structs.NodeService{Tags: []string{"foo"}},
			},
			structs.CheckServiceNode{
				Node:    &structs.Node{Node: "node2"},
				Service: &structs.NodeService{Tags: []string{"foo", "BAR"}},
			},
			structs.CheckServiceNode{
				Node: &structs.Node{Node: "node3"},
			},
			structs.CheckServiceNode{
				Node:    &structs.Node{Node: "node4"},
				Service: &structs.NodeService{Tags: []string{"foo", "baz"}},
			},
			structs.CheckServiceNode{
				Node:    &structs.Node{Node: "node5"},
				Service: &structs.NodeService{Tags: []string{"foo", "zoo"}},
			},
			structs.CheckServiceNode{
				Node:    &structs.Node{Node: "node6"},
				Service: &structs.NodeService{Tags: []string{"bar"}},
			},
		}
	}

	stringify := func(nodes structs.CheckServiceNodes) string {
		var names []string
		for _, node := range nodes {
			names = append(names, node.Node.Node)
		}
		sort.Strings(names)
		return strings.Join(names, "|")
	}

	ret := stringify(tagFilter([]string{}, testNodes()))
	if ret != "node1|node2|node3|node4|node5|node6" {
		t.Fatalf("bad: %s", ret)
	}

	ret = stringify(tagFilter([]string{"foo"}, testNodes()))
	if ret != "node1|node2|node4|node5" {
		t.Fatalf("bad: %s", ret)
	}

	ret = stringify(tagFilter([]string{"!foo"}, testNodes()))
	if ret != "node3|node6" {
		t.Fatalf("bad: %s", ret)
	}

	ret = stringify(tagFilter([]string{"!foo", "bar"}, testNodes()))
	if ret != "node6" {
		t.Fatalf("bad: %s", ret)
	}

	ret = stringify(tagFilter([]string{"!foo", "!bar"}, testNodes()))
	if ret != "node3" {
		t.Fatalf("bad: %s", ret)
	}

	ret = stringify(tagFilter([]string{"nope"}, testNodes()))
	if ret != "" {
		t.Fatalf("bad: %s", ret)
	}

	ret = stringify(tagFilter([]string{"bar"}, testNodes()))
	if ret != "node2|node6" {
		t.Fatalf("bad: %s", ret)
	}

	ret = stringify(tagFilter([]string{"BAR"}, testNodes()))
	if ret != "node2|node6" {
		t.Fatalf("bad: %s", ret)
	}

	ret = stringify(tagFilter([]string{"bAr"}, testNodes()))
	if ret != "node2|node6" {
		t.Fatalf("bad: %s", ret)
	}

	ret = stringify(tagFilter([]string{""}, testNodes()))
	if ret != "" {
		t.Fatalf("bad: %s", ret)
	}
}

func TestPreparedQuery_Wrapper(t *testing.T) {
	t.Parallel()
	dir1, s1 := testServerWithConfig(t, func(c *Config) {
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
	})
	defer os.RemoveAll(dir1)
	defer s1.Shutdown()

	dir2, s2 := testServerWithConfig(t, func(c *Config) {
		c.Datacenter = "dc2"
		c.ACLDatacenter = "dc1"
		c.ACLMasterToken = "root"
		c.ACLDefaultPolicy = "deny"
	})
	defer os.RemoveAll(dir2)
	defer s2.Shutdown()

	testrpc.WaitForLeader(t, s1.RPC, "dc1")
	testrpc.WaitForLeader(t, s2.RPC, "dc2")

	joinWAN(t, s2, s1)

	wrapper := &queryServerWrapper{s1}
	wrapper.GetLogger().Printf("[DEBUG] Test")

	ret, err := wrapper.GetOtherDatacentersByDistance()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(ret) != 1 || ret[0] != "dc2" {
		t.Fatalf("bad: %v", ret)
	}

	if err := wrapper.ForwardDC("Status.Ping", "dc2", &struct{}{}, &struct{}{}); err != nil {
		t.Fatalf("err: %v", err)
	}
}

type mockQueryServer struct {
	Datacenters      []string
	DatacentersError error
	QueryLog         []string
	QueryFn          func(dc string, args interface{}, reply interface{}) error
	Logger           *log.Logger
	LogBuffer        *bytes.Buffer
}

func (m *mockQueryServer) JoinQueryLog() string {
	return strings.Join(m.QueryLog, "|")
}

func (m *mockQueryServer) GetLogger() *log.Logger {
	if m.Logger == nil {
		m.LogBuffer = new(bytes.Buffer)
		m.Logger = log.New(m.LogBuffer, "", 0)
	}
	return m.Logger
}

func (m *mockQueryServer) GetOtherDatacentersByDistance() ([]string, error) {
	return m.Datacenters, m.DatacentersError
}

func (m *mockQueryServer) ForwardDC(method, dc string, args interface{}, reply interface{}) error {
	m.QueryLog = append(m.QueryLog, fmt.Sprintf("%s:%s", dc, method))
	if ret, ok := reply.(*structs.PreparedQueryExecuteResponse); ok {
		ret.Datacenter = dc
	}
	if m.QueryFn != nil {
		return m.QueryFn(dc, args, reply)
	}
	return nil
}

func TestPreparedQuery_queryFailover(t *testing.T) {
	t.Parallel()
	query := &structs.PreparedQuery{
		Name: "test",
		Service: structs.ServiceQuery{
			Failover: structs.QueryDatacenterOptions{
				NearestN:    0,
				Datacenters: []string{""},
			},
		},
	}

	nodes := func() structs.CheckServiceNodes {
		return structs.CheckServiceNodes{
			structs.CheckServiceNode{
				Node: &structs.Node{Node: "node1"},
			},
			structs.CheckServiceNode{
				Node: &structs.Node{Node: "node2"},
			},
			structs.CheckServiceNode{
				Node: &structs.Node{Node: "node3"},
			},
		}
	}

	{
		mock := &mockQueryServer{
			Datacenters: []string{"dc1", "dc2", "dc3", "xxx", "dc4"},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := queryFailover(mock, query, 0, structs.QueryOptions{}, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(reply.Nodes) != 0 || reply.Datacenter != "" || reply.Failovers != 0 {
			t.Fatalf("bad: %v", reply)
		}
	}

	{
		mock := &mockQueryServer{
			Datacenters:      []string{"dc1", "dc2", "dc3", "xxx", "dc4"},
			DatacentersError: fmt.Errorf("XXX"),
		}

		var reply structs.PreparedQueryExecuteResponse
		err := queryFailover(mock, query, 0, structs.QueryOptions{}, &reply)
		if err == nil || !strings.Contains(err.Error(), "XXX") {
			t.Fatalf("bad: %v", err)
		}
		if len(reply.Nodes) != 0 || reply.Datacenter != "" || reply.Failovers != 0 {
			t.Fatalf("bad: %v", reply)
		}
	}

	query.Service.Failover.NearestN = 3
	{
		mock := &mockQueryServer{
			Datacenters: []string{},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := queryFailover(mock, query, 0, structs.QueryOptions{}, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(reply.Nodes) != 0 || reply.Datacenter != "" || reply.Failovers != 0 {
			t.Fatalf("bad: %v", reply)
		}
	}

	query.Service.Failover.NearestN = 3
	{
		mock := &mockQueryServer{
			Datacenters: []string{"dc1", "dc2", "dc3", "xxx", "dc4"},
			QueryFn: func(dc string, args interface{}, reply interface{}) error {
				ret := reply.(*structs.PreparedQueryExecuteResponse)
				if dc == "dc1" {
					ret.Nodes = nodes()
				}
				return nil
			},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := queryFailover(mock, query, 0, structs.QueryOptions{}, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(reply.Nodes) != 3 ||
			reply.Datacenter != "dc1" || reply.Failovers != 1 ||
			!reflect.DeepEqual(reply.Nodes, nodes()) {
			t.Fatalf("bad: %v", reply)
		}
		if queries := mock.JoinQueryLog(); queries != "dc1:PreparedQuery.ExecuteRemote" {
			t.Fatalf("bad: %s", queries)
		}
	}

	query.Service.Failover.NearestN = 3
	{
		mock := &mockQueryServer{
			Datacenters: []string{"dc1", "dc2", "dc3", "xxx", "dc4"},
			QueryFn: func(dc string, args interface{}, reply interface{}) error {
				ret := reply.(*structs.PreparedQueryExecuteResponse)
				if dc == "dc3" {
					ret.Nodes = nodes()
				}
				return nil
			},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := queryFailover(mock, query, 0, structs.QueryOptions{}, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(reply.Nodes) != 3 ||
			reply.Datacenter != "dc3" || reply.Failovers != 3 ||
			!reflect.DeepEqual(reply.Nodes, nodes()) {
			t.Fatalf("bad: %v", reply)
		}
		if queries := mock.JoinQueryLog(); queries != "dc1:PreparedQuery.ExecuteRemote|dc2:PreparedQuery.ExecuteRemote|dc3:PreparedQuery.ExecuteRemote" {
			t.Fatalf("bad: %s", queries)
		}
	}

	query.Service.Failover.NearestN = 4
	{
		mock := &mockQueryServer{
			Datacenters: []string{"dc1", "dc2", "dc3", "xxx", "dc4"},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := queryFailover(mock, query, 0, structs.QueryOptions{}, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(reply.Nodes) != 0 ||
			reply.Datacenter != "xxx" || reply.Failovers != 4 {
			t.Fatalf("bad: %v", reply)
		}
		if queries := mock.JoinQueryLog(); queries != "dc1:PreparedQuery.ExecuteRemote|dc2:PreparedQuery.ExecuteRemote|dc3:PreparedQuery.ExecuteRemote|xxx:PreparedQuery.ExecuteRemote" {
			t.Fatalf("bad: %s", queries)
		}
	}

	query.Service.Failover.NearestN = 2
	query.Service.Failover.Datacenters = []string{"dc4"}
	{
		mock := &mockQueryServer{
			Datacenters: []string{"dc1", "dc2", "dc3", "xxx", "dc4"},
			QueryFn: func(dc string, args interface{}, reply interface{}) error {
				ret := reply.(*structs.PreparedQueryExecuteResponse)
				if dc == "dc4" {
					ret.Nodes = nodes()
				}
				return nil
			},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := queryFailover(mock, query, 0, structs.QueryOptions{}, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(reply.Nodes) != 3 ||
			reply.Datacenter != "dc4" || reply.Failovers != 3 ||
			!reflect.DeepEqual(reply.Nodes, nodes()) {
			t.Fatalf("bad: %v", reply)
		}
		if queries := mock.JoinQueryLog(); queries != "dc1:PreparedQuery.ExecuteRemote|dc2:PreparedQuery.ExecuteRemote|dc4:PreparedQuery.ExecuteRemote" {
			t.Fatalf("bad: %s", queries)
		}
	}

	query.Service.Failover.NearestN = 2
	query.Service.Failover.Datacenters = []string{"dc4", "dc1"}
	{
		mock := &mockQueryServer{
			Datacenters: []string{"dc1", "dc2", "dc3", "xxx", "dc4"},
			QueryFn: func(dc string, args interface{}, reply interface{}) error {
				ret := reply.(*structs.PreparedQueryExecuteResponse)
				if dc == "dc4" {
					ret.Nodes = nodes()
				}
				return nil
			},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := queryFailover(mock, query, 0, structs.QueryOptions{}, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(reply.Nodes) != 3 ||
			reply.Datacenter != "dc4" || reply.Failovers != 3 ||
			!reflect.DeepEqual(reply.Nodes, nodes()) {
			t.Fatalf("bad: %v", reply)
		}
		if queries := mock.JoinQueryLog(); queries != "dc1:PreparedQuery.ExecuteRemote|dc2:PreparedQuery.ExecuteRemote|dc4:PreparedQuery.ExecuteRemote" {
			t.Fatalf("bad: %s", queries)
		}
	}

	query.Service.Failover.NearestN = 2
	query.Service.Failover.Datacenters = []string{"nope", "dc4", "dc1"}
	{
		mock := &mockQueryServer{
			Datacenters: []string{"dc1", "dc2", "dc3", "xxx", "dc4"},
			QueryFn: func(dc string, args interface{}, reply interface{}) error {
				ret := reply.(*structs.PreparedQueryExecuteResponse)
				if dc == "dc4" {
					ret.Nodes = nodes()
				}
				return nil
			},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := queryFailover(mock, query, 0, structs.QueryOptions{}, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(reply.Nodes) != 3 ||
			reply.Datacenter != "dc4" || reply.Failovers != 3 ||
			!reflect.DeepEqual(reply.Nodes, nodes()) {
			t.Fatalf("bad: %v", reply)
		}
		if queries := mock.JoinQueryLog(); queries != "dc1:PreparedQuery.ExecuteRemote|dc2:PreparedQuery.ExecuteRemote|dc4:PreparedQuery.ExecuteRemote" {
			t.Fatalf("bad: %s", queries)
		}
		if !strings.Contains(mock.LogBuffer.String(), "Skipping unknown datacenter") {
			t.Fatalf("bad: %s", mock.LogBuffer.String())
		}
	}

	query.Service.Failover.NearestN = 2
	query.Service.Failover.Datacenters = []string{"dc4", "dc1"}
	{
		mock := &mockQueryServer{
			Datacenters: []string{"dc1", "dc2", "dc3", "xxx", "dc4"},
			QueryFn: func(dc string, args interface{}, reply interface{}) error {
				ret := reply.(*structs.PreparedQueryExecuteResponse)
				if dc == "dc1" {
					return fmt.Errorf("XXX")
				} else if dc == "dc4" {
					ret.Nodes = nodes()
				}
				return nil
			},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := queryFailover(mock, query, 0, structs.QueryOptions{}, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(reply.Nodes) != 3 ||
			reply.Datacenter != "dc4" || reply.Failovers != 3 ||
			!reflect.DeepEqual(reply.Nodes, nodes()) {
			t.Fatalf("bad: %v", reply)
		}
		if queries := mock.JoinQueryLog(); queries != "dc1:PreparedQuery.ExecuteRemote|dc2:PreparedQuery.ExecuteRemote|dc4:PreparedQuery.ExecuteRemote" {
			t.Fatalf("bad: %s", queries)
		}
		if !strings.Contains(mock.LogBuffer.String(), "Failed querying") {
			t.Fatalf("bad: %s", mock.LogBuffer.String())
		}
	}

	query.Service.Failover.NearestN = 0
	query.Service.Failover.Datacenters = []string{"dc3", "xxx"}
	{
		mock := &mockQueryServer{
			Datacenters: []string{"dc1", "dc2", "dc3", "xxx", "dc4"},
			QueryFn: func(dc string, args interface{}, reply interface{}) error {
				ret := reply.(*structs.PreparedQueryExecuteResponse)
				if dc == "xxx" {
					ret.Nodes = nodes()
				}
				return nil
			},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := queryFailover(mock, query, 0, structs.QueryOptions{}, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(reply.Nodes) != 3 ||
			reply.Datacenter != "xxx" || reply.Failovers != 2 ||
			!reflect.DeepEqual(reply.Nodes, nodes()) {
			t.Fatalf("bad: %v", reply)
		}
		if queries := mock.JoinQueryLog(); queries != "dc3:PreparedQuery.ExecuteRemote|xxx:PreparedQuery.ExecuteRemote" {
			t.Fatalf("bad: %s", queries)
		}
	}

	query.Service.Failover.NearestN = 0
	query.Service.Failover.Datacenters = []string{"xxx"}
	{
		mock := &mockQueryServer{
			Datacenters: []string{"dc1", "dc2", "dc3", "xxx", "dc4"},
			QueryFn: func(dc string, args interface{}, reply interface{}) error {
				inp := args.(*structs.PreparedQueryExecuteRemoteRequest)
				ret := reply.(*structs.PreparedQueryExecuteResponse)
				if dc == "xxx" {
					if inp.Limit != 5 {
						t.Fatalf("bad: %d", inp.Limit)
					}
					if inp.RequireConsistent != true {
						t.Fatalf("bad: %v", inp.RequireConsistent)
					}
					ret.Nodes = nodes()
				}
				return nil
			},
		}

		var reply structs.PreparedQueryExecuteResponse
		if err := queryFailover(mock, query, 5, structs.QueryOptions{RequireConsistent: true}, &reply); err != nil {
			t.Fatalf("err: %v", err)
		}
		if len(reply.Nodes) != 3 ||
			reply.Datacenter != "xxx" || reply.Failovers != 1 ||
			!reflect.DeepEqual(reply.Nodes, nodes()) {
			t.Fatalf("bad: %v", reply)
		}
		if queries := mock.JoinQueryLog(); queries != "xxx:PreparedQuery.ExecuteRemote" {
			t.Fatalf("bad: %s", queries)
		}
	}
}
