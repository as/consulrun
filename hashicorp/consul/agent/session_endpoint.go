package agent

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/types"
)

const (
	lockDelayMinThreshold = 1000
)

type sessionCreateResponse struct {
	ID string
}

func (s *HTTPServer) SessionCreate(resp http.ResponseWriter, req *http.Request) (interface{}, error) {

	args := structs.SessionRequest{
		Op: structs.SessionCreate,
		Session: structs.Session{
			Node:      s.agent.config.NodeName,
			Checks:    []types.CheckID{structs.SerfCheckID},
			LockDelay: 15 * time.Second,
			Behavior:  structs.SessionKeysRelease,
			TTL:       "",
		},
	}
	s.parseDC(req, &args.Datacenter)
	s.parseToken(req, &args.Token)

	if req.ContentLength > 0 {
		fixup := func(raw interface{}) error {
			if err := FixupLockDelay(raw); err != nil {
				return err
			}
			if err := FixupChecks(raw, &args.Session); err != nil {
				return err
			}
			return nil
		}
		if err := decodeBody(req, &args.Session, fixup); err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(resp, "Request decode failed: %v", err)
			return nil, nil
		}
	}

	var out string
	if err := s.agent.RPC("Session.Apply", &args, &out); err != nil {
		return nil, err
	}

	return sessionCreateResponse{out}, nil
}

func FixupLockDelay(raw interface{}) error {
	rawMap, ok := raw.(map[string]interface{})
	if !ok {
		return nil
	}
	var key string
	for k := range rawMap {
		if strings.ToLower(k) == "lockdelay" {
			key = k
			break
		}
	}
	if key != "" {
		val := rawMap[key]

		if vStr, ok := val.(string); ok {
			dur, err := time.ParseDuration(vStr)
			if err != nil {
				return err
			}
			if dur < lockDelayMinThreshold {
				dur = dur * time.Second
			}
			rawMap[key] = dur
		}

		if vNum, ok := val.(float64); ok {
			dur := time.Duration(vNum)
			if dur < lockDelayMinThreshold {
				dur = dur * time.Second
			}
			rawMap[key] = dur
		}
	}
	return nil
}

func FixupChecks(raw interface{}, s *structs.Session) error {
	rawMap, ok := raw.(map[string]interface{})
	if !ok {
		return nil
	}
	for k := range rawMap {
		if strings.ToLower(k) == "checks" {

			s.Checks = nil
			return nil
		}
	}
	return nil
}

func (s *HTTPServer) SessionDestroy(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	args := structs.SessionRequest{
		Op: structs.SessionDestroy,
	}
	s.parseDC(req, &args.Datacenter)
	s.parseToken(req, &args.Token)

	args.Session.ID = strings.TrimPrefix(req.URL.Path, "/v1/session/destroy/")
	if args.Session.ID == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing session")
		return nil, nil
	}

	var out string
	if err := s.agent.RPC("Session.Apply", &args, &out); err != nil {
		return nil, err
	}
	return true, nil
}

func (s *HTTPServer) SessionRenew(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	args := structs.SessionSpecificRequest{}
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	args.Session = strings.TrimPrefix(req.URL.Path, "/v1/session/renew/")
	if args.Session == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing session")
		return nil, nil
	}

	var out structs.IndexedSessions
	if err := s.agent.RPC("Session.Renew", &args, &out); err != nil {
		return nil, err
	} else if out.Sessions == nil {
		resp.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(resp, "Session id '%s' not found", args.Session)
		return nil, nil
	}

	return out.Sessions, nil
}

func (s *HTTPServer) SessionGet(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	args := structs.SessionSpecificRequest{}
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	args.Session = strings.TrimPrefix(req.URL.Path, "/v1/session/info/")
	if args.Session == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing session")
		return nil, nil
	}

	var out structs.IndexedSessions
	defer setMeta(resp, &out.QueryMeta)
	if err := s.agent.RPC("Session.Get", &args, &out); err != nil {
		return nil, err
	}

	if out.Sessions == nil {
		out.Sessions = make(structs.Sessions, 0)
	}
	return out.Sessions, nil
}

func (s *HTTPServer) SessionList(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	args := structs.DCSpecificRequest{}
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	var out structs.IndexedSessions
	defer setMeta(resp, &out.QueryMeta)
	if err := s.agent.RPC("Session.List", &args, &out); err != nil {
		return nil, err
	}

	if out.Sessions == nil {
		out.Sessions = make(structs.Sessions, 0)
	}
	return out.Sessions, nil
}

func (s *HTTPServer) SessionsForNode(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	args := structs.NodeSpecificRequest{}
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	args.Node = strings.TrimPrefix(req.URL.Path, "/v1/session/node/")
	if args.Node == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing node name")
		return nil, nil
	}

	var out structs.IndexedSessions
	defer setMeta(resp, &out.QueryMeta)
	if err := s.agent.RPC("Session.NodeSessions", &args, &out); err != nil {
		return nil, err
	}

	if out.Sessions == nil {
		out.Sessions = make(structs.Sessions, 0)
	}
	return out.Sessions, nil
}
