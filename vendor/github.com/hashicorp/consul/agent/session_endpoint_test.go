package agent

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/api"
	"github.com/as/consulrun/hashicorp/consul/types"
	"github.com/pascaldekloe/goe/verify"
)

func verifySession(t *testing.T, a *TestAgent, want structs.Session) {
	t.Helper()

	args := &structs.SessionSpecificRequest{
		Datacenter: "dc1",
		Session:    want.ID,
	}
	var out structs.IndexedSessions
	if err := a.RPC("Session.Get", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(out.Sessions) != 1 {
		t.Fatalf("bad: %#v", out.Sessions)
	}

	got := *(out.Sessions[0])
	got.CreateIndex = 0
	got.ModifyIndex = 0
	verify.Values(t, "", got, want)
}

func TestSessionCreate(t *testing.T) {
	t.Parallel()
	a := NewTestAgent(t.Name(), "")
	defer a.Shutdown()

	args := &structs.RegisterRequest{
		Datacenter: "dc1",
		Node:       a.Config.NodeName,
		Address:    "127.0.0.1",
		Check: &structs.HealthCheck{
			CheckID:   "consul",
			Node:      a.Config.NodeName,
			Name:      "consul",
			ServiceID: "consul",
			Status:    api.HealthPassing,
		},
	}
	var out struct{}
	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	body := bytes.NewBuffer(nil)
	enc := json.NewEncoder(body)
	raw := map[string]interface{}{
		"Name":      "my-cool-session",
		"Node":      a.Config.NodeName,
		"Checks":    []types.CheckID{structs.SerfCheckID, "consul"},
		"LockDelay": "20s",
	}
	enc.Encode(raw)

	req, _ := http.NewRequest("PUT", "/v1/session/create", body)
	resp := httptest.NewRecorder()
	obj, err := a.srv.SessionCreate(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	want := structs.Session{
		ID:        obj.(sessionCreateResponse).ID,
		Name:      "my-cool-session",
		Node:      a.Config.NodeName,
		Checks:    []types.CheckID{structs.SerfCheckID, "consul"},
		LockDelay: 20 * time.Second,
		Behavior:  structs.SessionKeysRelease,
	}
	verifySession(t, a, want)
}

func TestSessionCreate_Delete(t *testing.T) {
	t.Parallel()
	a := NewTestAgent(t.Name(), "")
	defer a.Shutdown()

	args := &structs.RegisterRequest{
		Datacenter: "dc1",
		Node:       a.Config.NodeName,
		Address:    "127.0.0.1",
		Check: &structs.HealthCheck{
			CheckID:   "consul",
			Node:      a.Config.NodeName,
			Name:      "consul",
			ServiceID: "consul",
			Status:    api.HealthPassing,
		},
	}
	var out struct{}
	if err := a.RPC("Catalog.Register", args, &out); err != nil {
		t.Fatalf("err: %v", err)
	}

	body := bytes.NewBuffer(nil)
	enc := json.NewEncoder(body)
	raw := map[string]interface{}{
		"Name":      "my-cool-session",
		"Node":      a.Config.NodeName,
		"Checks":    []types.CheckID{structs.SerfCheckID, "consul"},
		"LockDelay": "20s",
		"Behavior":  structs.SessionKeysDelete,
	}
	enc.Encode(raw)

	req, _ := http.NewRequest("PUT", "/v1/session/create", body)
	resp := httptest.NewRecorder()
	obj, err := a.srv.SessionCreate(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	want := structs.Session{
		ID:        obj.(sessionCreateResponse).ID,
		Name:      "my-cool-session",
		Node:      a.Config.NodeName,
		Checks:    []types.CheckID{structs.SerfCheckID, "consul"},
		LockDelay: 20 * time.Second,
		Behavior:  structs.SessionKeysDelete,
	}
	verifySession(t, a, want)
}

func TestSessionCreate_DefaultCheck(t *testing.T) {
	t.Parallel()
	a := NewTestAgent(t.Name(), "")
	defer a.Shutdown()

	body := bytes.NewBuffer(nil)
	enc := json.NewEncoder(body)
	raw := map[string]interface{}{
		"Name":      "my-cool-session",
		"Node":      a.Config.NodeName,
		"LockDelay": "20s",
	}
	enc.Encode(raw)

	req, _ := http.NewRequest("PUT", "/v1/session/create", body)
	resp := httptest.NewRecorder()
	obj, err := a.srv.SessionCreate(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	want := structs.Session{
		ID:        obj.(sessionCreateResponse).ID,
		Name:      "my-cool-session",
		Node:      a.Config.NodeName,
		Checks:    []types.CheckID{structs.SerfCheckID},
		LockDelay: 20 * time.Second,
		Behavior:  structs.SessionKeysRelease,
	}
	verifySession(t, a, want)
}

func TestSessionCreate_NoCheck(t *testing.T) {
	t.Parallel()
	a := NewTestAgent(t.Name(), "")
	defer a.Shutdown()

	body := bytes.NewBuffer(nil)
	enc := json.NewEncoder(body)
	raw := map[string]interface{}{
		"Name":      "my-cool-session",
		"Node":      a.Config.NodeName,
		"Checks":    []types.CheckID{},
		"LockDelay": "20s",
	}
	enc.Encode(raw)

	req, _ := http.NewRequest("PUT", "/v1/session/create", body)
	resp := httptest.NewRecorder()
	obj, err := a.srv.SessionCreate(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	want := structs.Session{
		ID:        obj.(sessionCreateResponse).ID,
		Name:      "my-cool-session",
		Node:      a.Config.NodeName,
		Checks:    []types.CheckID{},
		LockDelay: 20 * time.Second,
		Behavior:  structs.SessionKeysRelease,
	}
	verifySession(t, a, want)
}

func TestFixupLockDelay(t *testing.T) {
	t.Parallel()
	inp := map[string]interface{}{
		"lockdelay": float64(15),
	}
	if err := FixupLockDelay(inp); err != nil {
		t.Fatalf("err: %v", err)
	}
	if inp["lockdelay"] != 15*time.Second {
		t.Fatalf("bad: %v", inp)
	}

	inp = map[string]interface{}{
		"lockDelay": float64(15 * time.Second),
	}
	if err := FixupLockDelay(inp); err != nil {
		t.Fatalf("err: %v", err)
	}
	if inp["lockDelay"] != 15*time.Second {
		t.Fatalf("bad: %v", inp)
	}

	inp = map[string]interface{}{
		"LockDelay": "15s",
	}
	if err := FixupLockDelay(inp); err != nil {
		t.Fatalf("err: %v", err)
	}
	if inp["LockDelay"] != 15*time.Second {
		t.Fatalf("bad: %v", inp)
	}
}

func makeTestSession(t *testing.T, srv *HTTPServer) string {
	req, _ := http.NewRequest("PUT", "/v1/session/create", nil)
	resp := httptest.NewRecorder()
	obj, err := srv.SessionCreate(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sessResp := obj.(sessionCreateResponse)
	return sessResp.ID
}

func makeTestSessionDelete(t *testing.T, srv *HTTPServer) string {

	body := bytes.NewBuffer(nil)
	enc := json.NewEncoder(body)
	raw := map[string]interface{}{
		"Behavior": "delete",
	}
	enc.Encode(raw)

	req, _ := http.NewRequest("PUT", "/v1/session/create", body)
	resp := httptest.NewRecorder()
	obj, err := srv.SessionCreate(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sessResp := obj.(sessionCreateResponse)
	return sessResp.ID
}

func makeTestSessionTTL(t *testing.T, srv *HTTPServer, ttl string) string {

	body := bytes.NewBuffer(nil)
	enc := json.NewEncoder(body)
	raw := map[string]interface{}{
		"TTL": ttl,
	}
	enc.Encode(raw)

	req, _ := http.NewRequest("PUT", "/v1/session/create", body)
	resp := httptest.NewRecorder()
	obj, err := srv.SessionCreate(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sessResp := obj.(sessionCreateResponse)
	return sessResp.ID
}

func TestSessionDestroy(t *testing.T) {
	t.Parallel()
	a := NewTestAgent(t.Name(), "")
	defer a.Shutdown()

	id := makeTestSession(t, a.srv)

	req, _ := http.NewRequest("PUT", "/v1/session/destroy/"+id, nil)
	resp := httptest.NewRecorder()
	obj, err := a.srv.SessionDestroy(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if resp := obj.(bool); !resp {
		t.Fatalf("should work")
	}
}

func TestSessionCustomTTL(t *testing.T) {
	t.Parallel()
	ttl := 250 * time.Millisecond
	a := NewTestAgent(t.Name(), `
		session_ttl_min = "250ms"
	`)
	defer a.Shutdown()

	id := makeTestSessionTTL(t, a.srv, ttl.String())

	req, _ := http.NewRequest("GET", "/v1/session/info/"+id, nil)
	resp := httptest.NewRecorder()
	obj, err := a.srv.SessionGet(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	respObj, ok := obj.(structs.Sessions)
	if !ok {
		t.Fatalf("should work")
	}
	if len(respObj) != 1 {
		t.Fatalf("bad: %v", respObj)
	}
	if respObj[0].TTL != ttl.String() {
		t.Fatalf("Incorrect TTL: %s", respObj[0].TTL)
	}

	time.Sleep(ttl*structs.SessionTTLMultiplier + ttl)

	req, _ = http.NewRequest("GET", "/v1/session/info/"+id, nil)
	resp = httptest.NewRecorder()
	obj, err = a.srv.SessionGet(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	respObj, ok = obj.(structs.Sessions)
	if len(respObj) != 0 {
		t.Fatalf("session '%s' should have been destroyed", id)
	}
}

func TestSessionTTLRenew(t *testing.T) {

	ttl := 250 * time.Millisecond
	a := NewTestAgent(t.Name(), `
		session_ttl_min = "250ms"
	`)
	defer a.Shutdown()

	id := makeTestSessionTTL(t, a.srv, ttl.String())

	req, _ := http.NewRequest("GET", "/v1/session/info/"+id, nil)
	resp := httptest.NewRecorder()
	obj, err := a.srv.SessionGet(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	respObj, ok := obj.(structs.Sessions)
	if !ok {
		t.Fatalf("should work")
	}
	if len(respObj) != 1 {
		t.Fatalf("bad: %v", respObj)
	}
	if respObj[0].TTL != ttl.String() {
		t.Fatalf("Incorrect TTL: %s", respObj[0].TTL)
	}

	time.Sleep(ttl * (structs.SessionTTLMultiplier / 2))

	req, _ = http.NewRequest("PUT", "/v1/session/renew/"+id, nil)
	resp = httptest.NewRecorder()
	obj, err = a.srv.SessionRenew(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	respObj, ok = obj.(structs.Sessions)
	if !ok {
		t.Fatalf("should work")
	}
	if len(respObj) != 1 {
		t.Fatalf("bad: %v", respObj)
	}

	time.Sleep(ttl * structs.SessionTTLMultiplier)

	req, _ = http.NewRequest("GET", "/v1/session/info/"+id, nil)
	resp = httptest.NewRecorder()
	obj, err = a.srv.SessionGet(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	respObj, ok = obj.(structs.Sessions)
	if !ok {
		t.Fatalf("session '%s' should have renewed", id)
	}
	if len(respObj) != 1 {
		t.Fatalf("session '%s' should have renewed", id)
	}

	time.Sleep(ttl * structs.SessionTTLMultiplier)

	req, _ = http.NewRequest("GET", "/v1/session/info/"+id, nil)
	resp = httptest.NewRecorder()
	obj, err = a.srv.SessionGet(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	respObj, ok = obj.(structs.Sessions)
	if !ok {
		t.Fatalf("session '%s' should have destroyed", id)
	}
	if len(respObj) != 0 {
		t.Fatalf("session '%s' should have destroyed", id)
	}
}

func TestSessionGet(t *testing.T) {
	t.Parallel()
	t.Run("", func(t *testing.T) {
		a := NewTestAgent(t.Name(), "")
		defer a.Shutdown()

		req, _ := http.NewRequest("GET", "/v1/session/info/adf4238a-882b-9ddc-4a9d-5b6758e4159e", nil)
		resp := httptest.NewRecorder()
		obj, err := a.srv.SessionGet(resp, req)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respObj, ok := obj.(structs.Sessions)
		if !ok {
			t.Fatalf("should work")
		}
		if respObj == nil || len(respObj) != 0 {
			t.Fatalf("bad: %v", respObj)
		}
	})

	t.Run("", func(t *testing.T) {
		a := NewTestAgent(t.Name(), "")
		defer a.Shutdown()

		id := makeTestSession(t, a.srv)

		req, _ := http.NewRequest("GET", "/v1/session/info/"+id, nil)
		resp := httptest.NewRecorder()
		obj, err := a.srv.SessionGet(resp, req)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respObj, ok := obj.(structs.Sessions)
		if !ok {
			t.Fatalf("should work")
		}
		if len(respObj) != 1 {
			t.Fatalf("bad: %v", respObj)
		}
	})
}

func TestSessionList(t *testing.T) {
	t.Parallel()
	t.Run("", func(t *testing.T) {
		a := NewTestAgent(t.Name(), "")
		defer a.Shutdown()

		req, _ := http.NewRequest("GET", "/v1/session/list", nil)
		resp := httptest.NewRecorder()
		obj, err := a.srv.SessionList(resp, req)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respObj, ok := obj.(structs.Sessions)
		if !ok {
			t.Fatalf("should work")
		}
		if respObj == nil || len(respObj) != 0 {
			t.Fatalf("bad: %v", respObj)
		}
	})

	t.Run("", func(t *testing.T) {
		a := NewTestAgent(t.Name(), "")
		defer a.Shutdown()

		var ids []string
		for i := 0; i < 10; i++ {
			ids = append(ids, makeTestSession(t, a.srv))
		}

		req, _ := http.NewRequest("GET", "/v1/session/list", nil)
		resp := httptest.NewRecorder()
		obj, err := a.srv.SessionList(resp, req)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respObj, ok := obj.(structs.Sessions)
		if !ok {
			t.Fatalf("should work")
		}
		if len(respObj) != 10 {
			t.Fatalf("bad: %v", respObj)
		}
	})
}

func TestSessionsForNode(t *testing.T) {
	t.Parallel()
	t.Run("", func(t *testing.T) {
		a := NewTestAgent(t.Name(), "")
		defer a.Shutdown()

		req, _ := http.NewRequest("GET", "/v1/session/node/"+a.Config.NodeName, nil)
		resp := httptest.NewRecorder()
		obj, err := a.srv.SessionsForNode(resp, req)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respObj, ok := obj.(structs.Sessions)
		if !ok {
			t.Fatalf("should work")
		}
		if respObj == nil || len(respObj) != 0 {
			t.Fatalf("bad: %v", respObj)
		}
	})

	t.Run("", func(t *testing.T) {
		a := NewTestAgent(t.Name(), "")
		defer a.Shutdown()

		var ids []string
		for i := 0; i < 10; i++ {
			ids = append(ids, makeTestSession(t, a.srv))
		}

		req, _ := http.NewRequest("GET", "/v1/session/node/"+a.Config.NodeName, nil)
		resp := httptest.NewRecorder()
		obj, err := a.srv.SessionsForNode(resp, req)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respObj, ok := obj.(structs.Sessions)
		if !ok {
			t.Fatalf("should work")
		}
		if len(respObj) != 10 {
			t.Fatalf("bad: %v", respObj)
		}
	})
}

func TestSessionDeleteDestroy(t *testing.T) {
	t.Parallel()
	a := NewTestAgent(t.Name(), "")
	defer a.Shutdown()

	id := makeTestSessionDelete(t, a.srv)

	buf := bytes.NewBuffer([]byte("test"))
	req, _ := http.NewRequest("PUT", "/v1/kv/ephemeral?acquire="+id, buf)
	resp := httptest.NewRecorder()
	obj, err := a.srv.KVSEndpoint(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if res := obj.(bool); !res {
		t.Fatalf("should work")
	}

	req, _ = http.NewRequest("PUT", "/v1/session/destroy/"+id, nil)
	resp = httptest.NewRecorder()
	obj, err = a.srv.SessionDestroy(resp, req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if resp := obj.(bool); !resp {
		t.Fatalf("should work")
	}

	req, _ = http.NewRequest("GET", "/v1/kv/ephemeral", nil)
	resp = httptest.NewRecorder()
	obj, _ = a.srv.KVSEndpoint(resp, req)
	res, found := obj.(structs.DirEntries)
	if found || len(res) != 0 {
		t.Fatalf("bad: %v found, should be nothing", res)
	}
}
