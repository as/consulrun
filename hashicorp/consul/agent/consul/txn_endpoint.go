package consul

import (
	"fmt"
	"time"

	"github.com/armon/go-metrics"
	"github.com/as/consulrun/hashicorp/consul/acl"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
)

type Txn struct {
	srv *Server
}

func (t *Txn) preCheck(acl acl.ACL, ops structs.TxnOps) structs.TxnErrors {
	var errors structs.TxnErrors

	for i, op := range ops {
		if op.KV != nil {
			ok, err := kvsPreApply(t.srv, acl, op.KV.Verb, &op.KV.DirEnt)
			if err != nil {
				errors = append(errors, &structs.TxnError{
					OpIndex: i,
					What:    err.Error(),
				})
			} else if !ok {
				err = fmt.Errorf("failed to lock key %q due to lock delay", op.KV.DirEnt.Key)
				errors = append(errors, &structs.TxnError{
					OpIndex: i,
					What:    err.Error(),
				})
			}
		}
	}

	return errors
}

func (t *Txn) Apply(args *structs.TxnRequest, reply *structs.TxnResponse) error {
	if done, err := t.srv.forward("Txn.Apply", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"consul", "txn", "apply"}, time.Now())
	defer metrics.MeasureSince([]string{"txn", "apply"}, time.Now())

	acl, err := t.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	reply.Errors = t.preCheck(acl, args.Ops)
	if len(reply.Errors) > 0 {
		return nil
	}

	resp, err := t.srv.raftApply(structs.TxnRequestType, args)
	if err != nil {
		t.srv.logger.Printf("[ERR] consul.txn: Apply failed: %v", err)
		return err
	}
	if respErr, ok := resp.(error); ok {
		return respErr
	}

	if txnResp, ok := resp.(structs.TxnResponse); ok {
		if acl != nil {
			txnResp.Results = FilterTxnResults(acl, txnResp.Results)
		}
		*reply = txnResp
	} else {
		return fmt.Errorf("unexpected return type %T", resp)
	}
	return nil
}

func (t *Txn) Read(args *structs.TxnReadRequest, reply *structs.TxnReadResponse) error {
	if done, err := t.srv.forward("Txn.Read", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"consul", "txn", "read"}, time.Now())
	defer metrics.MeasureSince([]string{"txn", "read"}, time.Now())

	t.srv.setQueryMeta(&reply.QueryMeta)
	if args.RequireConsistent {
		if err := t.srv.consistentRead(); err != nil {
			return err
		}
	}

	acl, err := t.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	reply.Errors = t.preCheck(acl, args.Ops)
	if len(reply.Errors) > 0 {
		return nil
	}

	state := t.srv.fsm.State()
	reply.Results, reply.Errors = state.TxnRO(args.Ops)
	if acl != nil {
		reply.Results = FilterTxnResults(acl, reply.Results)
	}
	return nil
}
