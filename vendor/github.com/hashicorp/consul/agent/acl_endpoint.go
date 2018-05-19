package agent

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/structs"
)

type aclCreateResponse struct {
	ID string
}

func (s *HTTPServer) checkACLDisabled(resp http.ResponseWriter, req *http.Request) bool {
	if s.agent.config.ACLDatacenter != "" {
		return false
	}

	resp.WriteHeader(http.StatusUnauthorized)
	fmt.Fprint(resp, "ACL support disabled")
	return true
}

func (s *HTTPServer) ACLBootstrap(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if s.checkACLDisabled(resp, req) {
		return nil, nil
	}

	args := structs.DCSpecificRequest{
		Datacenter: s.agent.config.ACLDatacenter,
	}

	var out structs.ACL
	err := s.agent.RPC("ACL.Bootstrap", &args, &out)
	if err != nil {
		if strings.Contains(err.Error(), structs.ACLBootstrapNotAllowedErr.Error()) {
			resp.WriteHeader(http.StatusForbidden)
			fmt.Fprint(resp, acl.PermissionDeniedError{Cause: err.Error()}.Error())
			return nil, nil
		} else {
			return nil, err
		}
	}

	return aclCreateResponse{out.ID}, nil
}

func (s *HTTPServer) ACLDestroy(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if s.checkACLDisabled(resp, req) {
		return nil, nil
	}

	args := structs.ACLRequest{
		Datacenter: s.agent.config.ACLDatacenter,
		Op:         structs.ACLDelete,
	}
	s.parseToken(req, &args.Token)

	args.ACL.ID = strings.TrimPrefix(req.URL.Path, "/v1/acl/destroy/")
	if args.ACL.ID == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing ACL")
		return nil, nil
	}

	var out string
	if err := s.agent.RPC("ACL.Apply", &args, &out); err != nil {
		return nil, err
	}
	return true, nil
}

func (s *HTTPServer) ACLCreate(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if s.checkACLDisabled(resp, req) {
		return nil, nil
	}
	return s.aclSet(resp, req, false)
}

func (s *HTTPServer) ACLUpdate(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if s.checkACLDisabled(resp, req) {
		return nil, nil
	}
	return s.aclSet(resp, req, true)
}

func (s *HTTPServer) aclSet(resp http.ResponseWriter, req *http.Request, update bool) (interface{}, error) {
	args := structs.ACLRequest{
		Datacenter: s.agent.config.ACLDatacenter,
		Op:         structs.ACLSet,
		ACL: structs.ACL{
			Type: structs.ACLTypeClient,
		},
	}
	s.parseToken(req, &args.Token)

	if req.ContentLength > 0 {
		if err := decodeBody(req, &args.ACL, nil); err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(resp, "Request decode failed: %v", err)
			return nil, nil
		}
	}

	if update && args.ACL.ID == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "ACL ID must be set")
		return nil, nil
	}

	var out string
	if err := s.agent.RPC("ACL.Apply", &args, &out); err != nil {
		return nil, err
	}

	return aclCreateResponse{out}, nil
}

func (s *HTTPServer) ACLClone(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if s.checkACLDisabled(resp, req) {
		return nil, nil
	}

	args := structs.ACLSpecificRequest{
		Datacenter: s.agent.config.ACLDatacenter,
	}
	var dc string
	if done := s.parse(resp, req, &dc, &args.QueryOptions); done {
		return nil, nil
	}

	args.ACL = strings.TrimPrefix(req.URL.Path, "/v1/acl/clone/")
	if args.ACL == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing ACL")
		return nil, nil
	}

	var out structs.IndexedACLs
	defer setMeta(resp, &out.QueryMeta)
	if err := s.agent.RPC("ACL.Get", &args, &out); err != nil {
		return nil, err
	}

	if len(out.ACLs) == 0 {
		return nil, acl.ErrPermissionDenied
	}

	createArgs := structs.ACLRequest{
		Datacenter: args.Datacenter,
		Op:         structs.ACLSet,
		ACL:        *out.ACLs[0],
	}
	createArgs.ACL.ID = ""
	createArgs.Token = args.Token

	var outID string
	if err := s.agent.RPC("ACL.Apply", &createArgs, &outID); err != nil {
		return nil, err
	}

	return aclCreateResponse{outID}, nil
}

func (s *HTTPServer) ACLGet(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if s.checkACLDisabled(resp, req) {
		return nil, nil
	}

	args := structs.ACLSpecificRequest{
		Datacenter: s.agent.config.ACLDatacenter,
	}
	var dc string
	if done := s.parse(resp, req, &dc, &args.QueryOptions); done {
		return nil, nil
	}

	args.ACL = strings.TrimPrefix(req.URL.Path, "/v1/acl/info/")
	if args.ACL == "" {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Missing ACL")
		return nil, nil
	}

	var out structs.IndexedACLs
	defer setMeta(resp, &out.QueryMeta)
	if err := s.agent.RPC("ACL.Get", &args, &out); err != nil {
		return nil, err
	}

	if out.ACLs == nil {
		out.ACLs = make(structs.ACLs, 0)
	}
	return out.ACLs, nil
}

func (s *HTTPServer) ACLList(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if s.checkACLDisabled(resp, req) {
		return nil, nil
	}

	args := structs.DCSpecificRequest{
		Datacenter: s.agent.config.ACLDatacenter,
	}
	var dc string
	if done := s.parse(resp, req, &dc, &args.QueryOptions); done {
		return nil, nil
	}

	var out structs.IndexedACLs
	defer setMeta(resp, &out.QueryMeta)
	if err := s.agent.RPC("ACL.List", &args, &out); err != nil {
		return nil, err
	}

	if out.ACLs == nil {
		out.ACLs = make(structs.ACLs, 0)
	}
	return out.ACLs, nil
}

func (s *HTTPServer) ACLReplicationStatus(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if s.checkACLDisabled(resp, req) {
		return nil, nil
	}

	args := structs.DCSpecificRequest{}
	s.parseSource(req, &args.Source)
	if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
		return nil, nil
	}

	var out structs.ACLReplicationStatus
	if err := s.agent.RPC("ACL.ReplicationStatus", &args, &out); err != nil {
		return nil, err
	}
	return out, nil
}
