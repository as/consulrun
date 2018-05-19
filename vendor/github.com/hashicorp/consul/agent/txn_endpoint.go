package agent

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"

	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/api"
)

const (
	maxTxnOps = 64
)

func decodeValue(rawKV interface{}) error {
	rawMap, ok := rawKV.(map[string]interface{})
	if !ok {
		return fmt.Errorf("unexpected raw KV type: %T", rawKV)
	}
	for k, v := range rawMap {
		switch strings.ToLower(k) {
		case "value":

			if v == nil {
				return nil
			}

			s, ok := v.(string)
			if !ok {
				return fmt.Errorf("unexpected value type: %T", v)
			}
			decoded, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return fmt.Errorf("failed to decode value: %v", err)
			}
			rawMap[k] = decoded
			return nil
		}
	}
	return nil
}

func fixupKVOp(rawOp interface{}) error {
	rawMap, ok := rawOp.(map[string]interface{})
	if !ok {
		return fmt.Errorf("unexpected raw op type: %T", rawOp)
	}
	for k, v := range rawMap {
		switch strings.ToLower(k) {
		case "kv":
			if v == nil {
				return nil
			}
			return decodeValue(v)
		}
	}
	return nil
}

func fixupKVOps(raw interface{}) error {
	rawSlice, ok := raw.([]interface{})
	if !ok {
		return fmt.Errorf("unexpected raw type: %t", raw)
	}
	for _, rawOp := range rawSlice {
		if err := fixupKVOp(rawOp); err != nil {
			return err
		}
	}
	return nil
}

func isWrite(op api.KVOp) bool {
	switch op {
	case api.KVSet, api.KVDelete, api.KVDeleteCAS, api.KVDeleteTree, api.KVCAS, api.KVLock, api.KVUnlock:
		return true
	}
	return false
}

func (s *HTTPServer) convertOps(resp http.ResponseWriter, req *http.Request) (structs.TxnOps, int, bool) {

	var ops api.TxnOps
	if err := decodeBody(req, &ops, fixupKVOps); err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(resp, "Failed to parse body: %v", err)
		return nil, 0, false
	}

	if size := len(ops); size > maxTxnOps {
		resp.WriteHeader(http.StatusRequestEntityTooLarge)
		fmt.Fprintf(resp, "Transaction contains too many operations (%d > %d)",
			size, maxTxnOps)

		return nil, 0, false
	}

	var opsRPC structs.TxnOps
	var writes int
	var netKVSize int
	for _, in := range ops {
		if in.KV != nil {
			size := len(in.KV.Value)
			if size > maxKVSize {
				resp.WriteHeader(http.StatusRequestEntityTooLarge)
				fmt.Fprintf(resp, "Value for key %q is too large (%d > %d bytes)", in.KV.Key, size, maxKVSize)
				return nil, 0, false
			}
			netKVSize += size

			verb := api.KVOp(in.KV.Verb)
			if isWrite(verb) {
				writes++
			}

			out := &structs.TxnOp{
				KV: &structs.TxnKVOp{
					Verb: verb,
					DirEnt: structs.DirEntry{
						Key:     in.KV.Key,
						Value:   in.KV.Value,
						Flags:   in.KV.Flags,
						Session: in.KV.Session,
						RaftIndex: structs.RaftIndex{
							ModifyIndex: in.KV.Index,
						},
					},
				},
			}
			opsRPC = append(opsRPC, out)
		}
	}

	if netKVSize > maxKVSize {
		resp.WriteHeader(http.StatusRequestEntityTooLarge)
		fmt.Fprintf(resp, "Cumulative size of key data is too large (%d > %d bytes)",
			netKVSize, maxKVSize)

		return nil, 0, false
	}

	return opsRPC, writes, true
}

func (s *HTTPServer) Txn(resp http.ResponseWriter, req *http.Request) (interface{}, error) {

	ops, writes, ok := s.convertOps(resp, req)
	if !ok {
		return nil, nil
	}

	conflict := false
	var ret interface{}
	if writes == 0 {
		args := structs.TxnReadRequest{Ops: ops}
		if done := s.parse(resp, req, &args.Datacenter, &args.QueryOptions); done {
			return nil, nil
		}

		var reply structs.TxnReadResponse
		if err := s.agent.RPC("Txn.Read", &args, &reply); err != nil {
			return nil, err
		}

		setLastContact(resp, reply.LastContact)
		setKnownLeader(resp, reply.KnownLeader)

		ret, conflict = reply, len(reply.Errors) > 0
	} else {
		args := structs.TxnRequest{Ops: ops}
		s.parseDC(req, &args.Datacenter)
		s.parseToken(req, &args.Token)

		var reply structs.TxnResponse
		if err := s.agent.RPC("Txn.Apply", &args, &reply); err != nil {
			return nil, err
		}
		ret, conflict = reply, len(reply.Errors) > 0
	}

	if conflict {
		var buf []byte
		var err error
		buf, err = s.marshalJSON(req, ret)
		if err != nil {
			return nil, err
		}

		resp.Header().Set("Content-Type", "application/json")
		resp.WriteHeader(http.StatusConflict)
		resp.Write(buf)
		return nil, nil
	}

	return ret, nil
}
