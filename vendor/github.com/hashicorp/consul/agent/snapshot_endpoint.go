package agent

import (
	"bytes"
	"net/http"

	"github.com/hashicorp/consul/agent/structs"
)

func (s *HTTPServer) Snapshot(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	var args structs.SnapshotRequest
	s.parseDC(req, &args.Datacenter)
	s.parseToken(req, &args.Token)
	if _, ok := req.URL.Query()["stale"]; ok {
		args.AllowStale = true
	}

	switch req.Method {
	case "GET":
		args.Op = structs.SnapshotSave

		replyFn := func(reply *structs.SnapshotResponse) error {
			setMeta(resp, &reply.QueryMeta)
			return nil
		}

		var null bytes.Buffer
		if err := s.agent.SnapshotRPC(&args, &null, resp, replyFn); err != nil {
			return nil, err
		}
		return nil, nil

	case "PUT":
		args.Op = structs.SnapshotRestore
		if err := s.agent.SnapshotRPC(&args, req.Body, resp, nil); err != nil {
			return nil, err
		}
		return nil, nil

	default:
		return nil, MethodNotAllowedError{req.Method, []string{"GET", "PUT"}}
	}
}
