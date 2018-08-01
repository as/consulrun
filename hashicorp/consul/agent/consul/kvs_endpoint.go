package consul

import (
	"fmt"
	"time"

	"github.com/armon/go-metrics"
	"github.com/as/consulrun/hashicorp/consul/acl"
	"github.com/as/consulrun/hashicorp/consul/agent/consul/state"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/api"
	"github.com/as/consulrun/hashicorp/consul/sentinel"
	"github.com/as/consulrun/hashicorp/go-memdb"
)

type KVS struct {
	srv *Server
}

func kvsPreApply(srv *Server, rule acl.ACL, op api.KVOp, dirEnt *structs.DirEntry) (bool, error) {

	if dirEnt.Key == "" && op != api.KVDeleteTree {
		return false, fmt.Errorf("Must provide key")
	}

	if rule != nil {
		switch op {
		case api.KVDeleteTree:
			if !rule.KeyWritePrefix(dirEnt.Key) {
				return false, acl.ErrPermissionDenied
			}

		case api.KVGet, api.KVGetTree:

		case api.KVCheckSession, api.KVCheckIndex:

			if !rule.KeyRead(dirEnt.Key) {
				return false, acl.ErrPermissionDenied
			}

		default:
			scope := func() map[string]interface{} {
				return sentinel.ScopeKVUpsert(dirEnt.Key, dirEnt.Value, dirEnt.Flags)
			}
			if !rule.KeyWrite(dirEnt.Key, scope) {
				return false, acl.ErrPermissionDenied
			}
		}
	}

	if op == api.KVLock {
		state := srv.fsm.State()
		expires := state.KVSLockDelay(dirEnt.Key)
		if expires.After(time.Now()) {
			srv.logger.Printf("[WARN] consul.kvs: Rejecting lock of %s due to lock-delay until %v",
				dirEnt.Key, expires)
			return false, nil
		}
	}

	return true, nil
}

func (k *KVS) Apply(args *structs.KVSRequest, reply *bool) error {
	if done, err := k.srv.forward("KVS.Apply", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"consul", "kvs", "apply"}, time.Now())
	defer metrics.MeasureSince([]string{"kvs", "apply"}, time.Now())

	acl, err := k.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	ok, err := kvsPreApply(k.srv, acl, args.Op, &args.DirEnt)
	if err != nil {
		return err
	}
	if !ok {
		*reply = false
		return nil
	}

	resp, err := k.srv.raftApply(structs.KVSRequestType, args)
	if err != nil {
		k.srv.logger.Printf("[ERR] consul.kvs: Apply failed: %v", err)
		return err
	}
	if respErr, ok := resp.(error); ok {
		return respErr
	}

	if respBool, ok := resp.(bool); ok {
		*reply = respBool
	}
	return nil
}

func (k *KVS) Get(args *structs.KeyRequest, reply *structs.IndexedDirEntries) error {
	if done, err := k.srv.forward("KVS.Get", args, args, reply); done {
		return err
	}

	aclRule, err := k.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	return k.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, ent, err := state.KVSGet(ws, args.Key)
			if err != nil {
				return err
			}
			if aclRule != nil && !aclRule.KeyRead(args.Key) {
				return acl.ErrPermissionDenied
			}

			if ent == nil {

				if index == 0 {
					reply.Index = 1
				} else {
					reply.Index = index
				}
				reply.Entries = nil
			} else {
				reply.Index = ent.ModifyIndex
				reply.Entries = structs.DirEntries{ent}
			}
			return nil
		})
}

func (k *KVS) List(args *structs.KeyRequest, reply *structs.IndexedDirEntries) error {
	if done, err := k.srv.forward("KVS.List", args, args, reply); done {
		return err
	}

	aclToken, err := k.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}

	if aclToken != nil && k.srv.config.ACLEnableKeyListPolicy && !aclToken.KeyList(args.Key) {
		return acl.ErrPermissionDenied
	}

	return k.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, ent, err := state.KVSList(ws, args.Key)
			if err != nil {
				return err
			}
			if aclToken != nil {
				ent = FilterDirEnt(aclToken, ent)
			}

			if len(ent) == 0 {

				if index == 0 {
					reply.Index = 1
				} else {
					reply.Index = index
				}
				reply.Entries = nil
			} else {
				reply.Index = index
				reply.Entries = ent
			}
			return nil
		})
}

func (k *KVS) ListKeys(args *structs.KeyListRequest, reply *structs.IndexedKeyList) error {
	if done, err := k.srv.forward("KVS.ListKeys", args, args, reply); done {
		return err
	}

	aclToken, err := k.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}

	if aclToken != nil && k.srv.config.ACLEnableKeyListPolicy && !aclToken.KeyList(args.Prefix) {
		return acl.ErrPermissionDenied
	}

	return k.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, keys, err := state.KVSListKeys(ws, args.Prefix, args.Seperator)
			if err != nil {
				return err
			}

			if index == 0 {
				reply.Index = 1
			} else {
				reply.Index = index
			}

			if aclToken != nil {
				keys = FilterKeys(aclToken, keys)
			}
			reply.Keys = keys
			return nil
		})
}
