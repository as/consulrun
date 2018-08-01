package consul

import (
	"fmt"

	"github.com/as/consulrun/hashicorp/consul/acl"
	"github.com/as/consulrun/hashicorp/consul/agent/consul/state"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/go-memdb"
	"github.com/as/consulrun/hashicorp/go-multierror"
	"github.com/as/consulrun/hashicorp/serf/serf"
)

type Internal struct {
	srv *Server
}

func (m *Internal) NodeInfo(args *structs.NodeSpecificRequest,
	reply *structs.IndexedNodeDump) error {
	if done, err := m.srv.forward("Internal.NodeInfo", args, args, reply); done {
		return err
	}

	return m.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, dump, err := state.NodeInfo(ws, args.Node)
			if err != nil {
				return err
			}

			reply.Index, reply.Dump = index, dump
			return m.srv.filterACL(args.Token, reply)
		})
}

func (m *Internal) NodeDump(args *structs.DCSpecificRequest,
	reply *structs.IndexedNodeDump) error {
	if done, err := m.srv.forward("Internal.NodeDump", args, args, reply); done {
		return err
	}

	return m.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, dump, err := state.NodeDump(ws)
			if err != nil {
				return err
			}

			reply.Index, reply.Dump = index, dump
			return m.srv.filterACL(args.Token, reply)
		})
}

func (m *Internal) EventFire(args *structs.EventFireRequest,
	reply *structs.EventFireResponse) error {
	if done, err := m.srv.forward("Internal.EventFire", args, args, reply); done {
		return err
	}

	rule, err := m.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}

	if rule != nil && !rule.EventWrite(args.Name) {
		m.srv.logger.Printf("[WARN] consul: user event %q blocked by ACLs", args.Name)
		return acl.ErrPermissionDenied
	}

	m.srv.setQueryMeta(&reply.QueryMeta)

	eventName := userEventName(args.Name)

	segments := m.srv.LANSegments()
	var errs error
	for name, segment := range segments {
		err := segment.UserEvent(eventName, args.Payload, false)
		if err != nil {
			err = fmt.Errorf("error broadcasting event to segment %q: %v", name, err)
			errs = multierror.Append(errs, err)
		}
	}
	return errs
}

func (m *Internal) KeyringOperation(
	args *structs.KeyringRequest,
	reply *structs.KeyringResponses) error {

	rule, err := m.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	if rule != nil {
		switch args.Operation {
		case structs.KeyringList:
			if !rule.KeyringRead() {
				return fmt.Errorf("Reading keyring denied by ACLs")
			}
		case structs.KeyringInstall:
			fallthrough
		case structs.KeyringUse:
			fallthrough
		case structs.KeyringRemove:
			if !rule.KeyringWrite() {
				return fmt.Errorf("Modifying keyring denied due to ACLs")
			}
		default:
			panic("Invalid keyring operation")
		}
	}

	if !args.Forwarded && m.srv.serfWAN != nil {
		args.Forwarded = true
		m.executeKeyringOp(args, reply, true)
		return m.srv.globalRPC("Internal.KeyringOperation", args, reply)
	}

	m.executeKeyringOp(args, reply, false)
	return nil
}

func (m *Internal) executeKeyringOp(
	args *structs.KeyringRequest,
	reply *structs.KeyringResponses,
	wan bool) {

	if wan {
		mgr := m.srv.KeyManagerWAN()
		m.executeKeyringOpMgr(mgr, args, reply, wan, "")
	} else {
		segments := m.srv.LANSegments()
		for name, segment := range segments {
			mgr := segment.KeyManager()
			m.executeKeyringOpMgr(mgr, args, reply, wan, name)
		}
	}
}

func (m *Internal) executeKeyringOpMgr(
	mgr *serf.KeyManager,
	args *structs.KeyringRequest,
	reply *structs.KeyringResponses,
	wan bool,
	segment string) {
	var serfResp *serf.KeyResponse
	var err error

	opts := &serf.KeyRequestOptions{RelayFactor: args.RelayFactor}
	switch args.Operation {
	case structs.KeyringList:
		serfResp, err = mgr.ListKeysWithOptions(opts)
	case structs.KeyringInstall:
		serfResp, err = mgr.InstallKeyWithOptions(args.Key, opts)
	case structs.KeyringUse:
		serfResp, err = mgr.UseKeyWithOptions(args.Key, opts)
	case structs.KeyringRemove:
		serfResp, err = mgr.RemoveKeyWithOptions(args.Key, opts)
	}

	errStr := ""
	if err != nil {
		errStr = err.Error()
	}

	reply.Responses = append(reply.Responses, &structs.KeyringResponse{
		WAN:        wan,
		Datacenter: m.srv.config.Datacenter,
		Segment:    segment,
		Messages:   serfResp.Messages,
		Keys:       serfResp.Keys,
		NumNodes:   serfResp.NumNodes,
		Error:      errStr,
	})
}
