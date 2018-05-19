package consul

import (
	"fmt"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/go-uuid"
)

type ACL struct {
	srv *Server
}

func (a *ACL) Bootstrap(args *structs.DCSpecificRequest, reply *structs.ACL) error {
	if done, err := a.srv.forward("ACL.Bootstrap", args, args, reply); done {
		return err
	}

	if a.srv.config.ACLDatacenter != a.srv.config.Datacenter {
		return acl.ErrDisabled
	}

	state := a.srv.fsm.State()
	bs, err := state.ACLGetBootstrap()
	if err != nil {
		return err
	}
	if bs == nil {
		return structs.ACLBootstrapNotInitializedErr
	}
	if !bs.AllowBootstrap {
		return structs.ACLBootstrapNotAllowedErr
	}

	token, err := uuid.GenerateUUID()
	if err != nil {
		return fmt.Errorf("failed to make random token: %v", err)
	}

	req := structs.ACLRequest{
		Datacenter: a.srv.config.ACLDatacenter,
		Op:         structs.ACLBootstrapNow,
		ACL: structs.ACL{
			ID:   token,
			Name: "Bootstrap Token",
			Type: structs.ACLTypeManagement,
		},
	}
	resp, err := a.srv.raftApply(structs.ACLRequestType, &req)
	if err != nil {
		return err
	}
	switch v := resp.(type) {
	case error:
		return v

	case *structs.ACL:
		*reply = *v

	default:

		a.srv.logger.Printf("[ERR] consul.acl: Unexpected response during bootstrap: %T", v)
	}

	a.srv.logger.Printf("[INFO] consul.acl: ACL bootstrap completed")
	return nil
}

func aclApplyInternal(srv *Server, args *structs.ACLRequest, reply *string) error {

	if args.ACL.ID == "" {
		return fmt.Errorf("Missing ACL ID")
	}

	switch args.Op {
	case structs.ACLSet:

		switch args.ACL.Type {
		case structs.ACLTypeClient:
		case structs.ACLTypeManagement:
		default:
			return fmt.Errorf("Invalid ACL Type")
		}

		if acl.RootACL(args.ACL.ID) != nil {
			return acl.PermissionDeniedError{Cause: "Cannot modify root ACL"}
		}

		_, err := acl.Parse(args.ACL.Rules, srv.sentinel)
		if err != nil {
			return fmt.Errorf("ACL rule compilation failed: %v", err)
		}

	case structs.ACLDelete:
		if args.ACL.ID == anonymousToken {
			return acl.PermissionDeniedError{Cause: "Cannot delete anonymous token"}
		}

	default:
		return fmt.Errorf("Invalid ACL Operation")
	}

	resp, err := srv.raftApply(structs.ACLRequestType, args)
	if err != nil {
		srv.logger.Printf("[ERR] consul.acl: Apply failed: %v", err)
		return err
	}
	if respErr, ok := resp.(error); ok {
		return respErr
	}

	if respString, ok := resp.(string); ok {
		*reply = respString
	}

	return nil
}

func (a *ACL) Apply(args *structs.ACLRequest, reply *string) error {
	if done, err := a.srv.forward("ACL.Apply", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"consul", "acl", "apply"}, time.Now())
	defer metrics.MeasureSince([]string{"acl", "apply"}, time.Now())

	if a.srv.config.ACLDatacenter != a.srv.config.Datacenter {
		return acl.ErrDisabled
	}

	if rule, err := a.srv.resolveToken(args.Token); err != nil {
		return err
	} else if rule == nil || !rule.ACLModify() {
		return acl.ErrPermissionDenied
	}

	if args.Op == structs.ACLSet && args.ACL.ID == "" {
		state := a.srv.fsm.State()
		for {
			var err error
			args.ACL.ID, err = uuid.GenerateUUID()
			if err != nil {
				a.srv.logger.Printf("[ERR] consul.acl: UUID generation failed: %v", err)
				return err
			}

			_, acl, err := state.ACLGet(nil, args.ACL.ID)
			if err != nil {
				a.srv.logger.Printf("[ERR] consul.acl: ACL lookup failed: %v", err)
				return err
			}
			if acl == nil {
				break
			}
		}
	}

	if err := aclApplyInternal(a.srv, args, reply); err != nil {
		return err
	}

	if args.ACL.ID != "" {
		a.srv.aclAuthCache.ClearACL(args.ACL.ID)
	}

	return nil
}

func (a *ACL) Get(args *structs.ACLSpecificRequest,
	reply *structs.IndexedACLs) error {
	if done, err := a.srv.forward("ACL.Get", args, args, reply); done {
		return err
	}

	if a.srv.config.ACLDatacenter != a.srv.config.Datacenter {
		return acl.ErrDisabled
	}

	return a.srv.blockingQuery(&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, acl, err := state.ACLGet(ws, args.ACL)
			if err != nil {
				return err
			}

			reply.Index = index
			if acl != nil {
				reply.ACLs = structs.ACLs{acl}
			} else {
				reply.ACLs = nil
			}
			return nil
		})
}

func makeACLETag(parent string, policy *acl.Policy) string {
	return fmt.Sprintf("%s:%s", parent, policy.ID)
}

func (a *ACL) GetPolicy(args *structs.ACLPolicyRequest, reply *structs.ACLPolicy) error {
	if done, err := a.srv.forward("ACL.GetPolicy", args, args, reply); done {
		return err
	}

	if a.srv.config.ACLDatacenter != a.srv.config.Datacenter {
		return acl.ErrDisabled
	}

	parent, policy, err := a.srv.aclAuthCache.GetACLPolicy(args.ACL)
	if err != nil {
		return err
	}

	conf := a.srv.config
	etag := makeACLETag(parent, policy)

	reply.ETag = etag
	reply.TTL = conf.ACLTTL
	a.srv.setQueryMeta(&reply.QueryMeta)

	if args.ETag != etag {
		reply.Parent = parent
		reply.Policy = policy
	}
	return nil
}

func (a *ACL) List(args *structs.DCSpecificRequest,
	reply *structs.IndexedACLs) error {
	if done, err := a.srv.forward("ACL.List", args, args, reply); done {
		return err
	}

	if a.srv.config.ACLDatacenter != a.srv.config.Datacenter {
		return acl.ErrDisabled
	}

	if rule, err := a.srv.resolveToken(args.Token); err != nil {
		return err
	} else if rule == nil || !rule.ACLList() {
		return acl.ErrPermissionDenied
	}

	return a.srv.blockingQuery(&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, acls, err := state.ACLList(ws)
			if err != nil {
				return err
			}

			reply.Index, reply.ACLs = index, acls
			return nil
		})
}

func (a *ACL) ReplicationStatus(args *structs.DCSpecificRequest,
	reply *structs.ACLReplicationStatus) error {

	args.RequireConsistent = true
	args.AllowStale = false
	if done, err := a.srv.forward("ACL.ReplicationStatus", args, args, reply); done {
		return err
	}

	a.srv.aclReplicationStatusLock.RLock()
	*reply = a.srv.aclReplicationStatus
	a.srv.aclReplicationStatusLock.RUnlock()
	return nil
}
