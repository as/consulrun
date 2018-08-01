package structs

import (
	"fmt"

	"github.com/as/consulrun/hashicorp/consul/api"
)

type TxnKVOp struct {
	Verb   api.KVOp
	DirEnt DirEntry
}

type TxnKVResult *DirEntry

type TxnOp struct {
	KV *TxnKVOp
}

type TxnOps []*TxnOp

type TxnRequest struct {
	Datacenter string
	Ops        TxnOps
	WriteRequest
}

func (r *TxnRequest) RequestDatacenter() string {
	return r.Datacenter
}

type TxnReadRequest struct {
	Datacenter string
	Ops        TxnOps
	QueryOptions
}

func (r *TxnReadRequest) RequestDatacenter() string {
	return r.Datacenter
}

type TxnError struct {
	OpIndex int
	What    string
}

func (e TxnError) Error() string {
	return fmt.Sprintf("op %d: %s", e.OpIndex, e.What)
}

type TxnErrors []*TxnError

type TxnResult struct {
	KV TxnKVResult
}

type TxnResults []*TxnResult

type TxnResponse struct {
	Results TxnResults
	Errors  TxnErrors
}

type TxnReadResponse struct {
	TxnResponse
	QueryMeta
}
