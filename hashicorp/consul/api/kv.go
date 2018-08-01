package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type KVPair struct {
	Key string

	CreateIndex uint64

	ModifyIndex uint64

	LockIndex uint64

	Flags uint64

	Value []byte

	Session string
}

type KVPairs []*KVPair

type KVOp string

const (
	KVSet            KVOp = "set"
	KVDelete         KVOp = "delete"
	KVDeleteCAS      KVOp = "delete-cas"
	KVDeleteTree     KVOp = "delete-tree"
	KVCAS            KVOp = "cas"
	KVLock           KVOp = "lock"
	KVUnlock         KVOp = "unlock"
	KVGet            KVOp = "get"
	KVGetTree        KVOp = "get-tree"
	KVCheckSession   KVOp = "check-session"
	KVCheckIndex     KVOp = "check-index"
	KVCheckNotExists KVOp = "check-not-exists"
)

type KVTxnOp struct {
	Verb    KVOp
	Key     string
	Value   []byte
	Flags   uint64
	Index   uint64
	Session string
}

type KVTxnOps []*KVTxnOp

type KVTxnResponse struct {
	Results []*KVPair
	Errors  TxnErrors
}

type KV struct {
	c *Client
}

func (c *Client) KV() *KV {
	return &KV{c}
}

func (k *KV) Get(key string, q *QueryOptions) (*KVPair, *QueryMeta, error) {
	resp, qm, err := k.getInternal(key, nil, q)
	if err != nil {
		return nil, nil, err
	}
	if resp == nil {
		return nil, qm, nil
	}
	defer resp.Body.Close()

	var entries []*KVPair
	if err := decodeBody(resp, &entries); err != nil {
		return nil, nil, err
	}
	if len(entries) > 0 {
		return entries[0], qm, nil
	}
	return nil, qm, nil
}

func (k *KV) List(prefix string, q *QueryOptions) (KVPairs, *QueryMeta, error) {
	resp, qm, err := k.getInternal(prefix, map[string]string{"recurse": ""}, q)
	if err != nil {
		return nil, nil, err
	}
	if resp == nil {
		return nil, qm, nil
	}
	defer resp.Body.Close()

	var entries []*KVPair
	if err := decodeBody(resp, &entries); err != nil {
		return nil, nil, err
	}
	return entries, qm, nil
}

func (k *KV) Keys(prefix, separator string, q *QueryOptions) ([]string, *QueryMeta, error) {
	params := map[string]string{"keys": ""}
	if separator != "" {
		params["separator"] = separator
	}
	resp, qm, err := k.getInternal(prefix, params, q)
	if err != nil {
		return nil, nil, err
	}
	if resp == nil {
		return nil, qm, nil
	}
	defer resp.Body.Close()

	var entries []string
	if err := decodeBody(resp, &entries); err != nil {
		return nil, nil, err
	}
	return entries, qm, nil
}

func (k *KV) getInternal(key string, params map[string]string, q *QueryOptions) (*http.Response, *QueryMeta, error) {
	r := k.c.newRequest("GET", "/v1/kv/"+strings.TrimPrefix(key, "/"))
	r.setQueryOptions(q)
	for param, val := range params {
		r.params.Set(param, val)
	}
	rtt, resp, err := k.c.doRequest(r)
	if err != nil {
		return nil, nil, err
	}

	qm := &QueryMeta{}
	parseQueryMeta(resp, qm)
	qm.RequestTime = rtt

	if resp.StatusCode == 404 {
		resp.Body.Close()
		return nil, qm, nil
	} else if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, nil, fmt.Errorf("Unexpected response code: %d", resp.StatusCode)
	}
	return resp, qm, nil
}

func (k *KV) Put(p *KVPair, q *WriteOptions) (*WriteMeta, error) {
	params := make(map[string]string, 1)
	if p.Flags != 0 {
		params["flags"] = strconv.FormatUint(p.Flags, 10)
	}
	_, wm, err := k.put(p.Key, params, p.Value, q)
	return wm, err
}

func (k *KV) CAS(p *KVPair, q *WriteOptions) (bool, *WriteMeta, error) {
	params := make(map[string]string, 2)
	if p.Flags != 0 {
		params["flags"] = strconv.FormatUint(p.Flags, 10)
	}
	params["cas"] = strconv.FormatUint(p.ModifyIndex, 10)
	return k.put(p.Key, params, p.Value, q)
}

func (k *KV) Acquire(p *KVPair, q *WriteOptions) (bool, *WriteMeta, error) {
	params := make(map[string]string, 2)
	if p.Flags != 0 {
		params["flags"] = strconv.FormatUint(p.Flags, 10)
	}
	params["acquire"] = p.Session
	return k.put(p.Key, params, p.Value, q)
}

func (k *KV) Release(p *KVPair, q *WriteOptions) (bool, *WriteMeta, error) {
	params := make(map[string]string, 2)
	if p.Flags != 0 {
		params["flags"] = strconv.FormatUint(p.Flags, 10)
	}
	params["release"] = p.Session
	return k.put(p.Key, params, p.Value, q)
}

func (k *KV) put(key string, params map[string]string, body []byte, q *WriteOptions) (bool, *WriteMeta, error) {
	if len(key) > 0 && key[0] == '/' {
		return false, nil, fmt.Errorf("Invalid key. Key must not begin with a '/': %s", key)
	}

	r := k.c.newRequest("PUT", "/v1/kv/"+key)
	r.setWriteOptions(q)
	for param, val := range params {
		r.params.Set(param, val)
	}
	r.body = bytes.NewReader(body)
	rtt, resp, err := requireOK(k.c.doRequest(r))
	if err != nil {
		return false, nil, err
	}
	defer resp.Body.Close()

	qm := &WriteMeta{}
	qm.RequestTime = rtt

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		return false, nil, fmt.Errorf("Failed to read response: %v", err)
	}
	res := strings.Contains(buf.String(), "true")
	return res, qm, nil
}

func (k *KV) Delete(key string, w *WriteOptions) (*WriteMeta, error) {
	_, qm, err := k.deleteInternal(key, nil, w)
	return qm, err
}

func (k *KV) DeleteCAS(p *KVPair, q *WriteOptions) (bool, *WriteMeta, error) {
	params := map[string]string{
		"cas": strconv.FormatUint(p.ModifyIndex, 10),
	}
	return k.deleteInternal(p.Key, params, q)
}

func (k *KV) DeleteTree(prefix string, w *WriteOptions) (*WriteMeta, error) {
	_, qm, err := k.deleteInternal(prefix, map[string]string{"recurse": ""}, w)
	return qm, err
}

func (k *KV) deleteInternal(key string, params map[string]string, q *WriteOptions) (bool, *WriteMeta, error) {
	r := k.c.newRequest("DELETE", "/v1/kv/"+strings.TrimPrefix(key, "/"))
	r.setWriteOptions(q)
	for param, val := range params {
		r.params.Set(param, val)
	}
	rtt, resp, err := requireOK(k.c.doRequest(r))
	if err != nil {
		return false, nil, err
	}
	defer resp.Body.Close()

	qm := &WriteMeta{}
	qm.RequestTime = rtt

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		return false, nil, fmt.Errorf("Failed to read response: %v", err)
	}
	res := strings.Contains(buf.String(), "true")
	return res, qm, nil
}

type TxnOp struct {
	KV *KVTxnOp
}

type TxnOps []*TxnOp

type TxnResult struct {
	KV *KVPair
}

type TxnResults []*TxnResult

type TxnError struct {
	OpIndex int
	What    string
}

type TxnErrors []*TxnError

type TxnResponse struct {
	Results TxnResults
	Errors  TxnErrors
}

//
//
//
//
//
func (k *KV) Txn(txn KVTxnOps, q *QueryOptions) (bool, *KVTxnResponse, *QueryMeta, error) {
	r := k.c.newRequest("PUT", "/v1/txn")
	r.setQueryOptions(q)

	ops := make(TxnOps, 0, len(txn))
	for _, kvOp := range txn {
		ops = append(ops, &TxnOp{KV: kvOp})
	}
	r.obj = ops
	rtt, resp, err := k.c.doRequest(r)
	if err != nil {
		return false, nil, nil, err
	}
	defer resp.Body.Close()

	qm := &QueryMeta{}
	parseQueryMeta(resp, qm)
	qm.RequestTime = rtt

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusConflict {
		var txnResp TxnResponse
		if err := decodeBody(resp, &txnResp); err != nil {
			return false, nil, nil, err
		}

		kvResp := KVTxnResponse{
			Errors: txnResp.Errors,
		}
		for _, result := range txnResp.Results {
			kvResp.Results = append(kvResp.Results, result.KV)
		}
		return resp.StatusCode == http.StatusOK, &kvResp, qm, nil
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		return false, nil, nil, fmt.Errorf("Failed to read response: %v", err)
	}
	return false, nil, nil, fmt.Errorf("Failed request: %s", buf.String())
}
