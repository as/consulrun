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

type Session struct {
	srv *Server
}

func (s *Session) Apply(args *structs.SessionRequest, reply *string) error {
	if done, err := s.srv.forward("Session.Apply", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"consul", "session", "apply"}, time.Now())
	defer metrics.MeasureSince([]string{"session", "apply"}, time.Now())

	if args.Session.ID == "" && args.Op == structs.SessionDestroy {
		return fmt.Errorf("Must provide ID")
	}
	if args.Session.Node == "" && args.Op == structs.SessionCreate {
		return fmt.Errorf("Must provide Node")
	}

	rule, err := s.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	if rule != nil && s.srv.config.ACLEnforceVersion8 {
		switch args.Op {
		case structs.SessionDestroy:
			state := s.srv.fsm.State()
			_, existing, err := state.SessionGet(nil, args.Session.ID)
			if err != nil {
				return fmt.Errorf("Session lookup failed: %v", err)
			}
			if existing == nil {
				return fmt.Errorf("Unknown session %q", args.Session.ID)
			}
			if !rule.SessionWrite(existing.Node) {
				return acl.ErrPermissionDenied
			}

		case structs.SessionCreate:
			if !rule.SessionWrite(args.Session.Node) {
				return acl.ErrPermissionDenied
			}

		default:
			return fmt.Errorf("Invalid session operation %q", args.Op)
		}
	}

	switch args.Session.Behavior {
	case "":

		args.Session.Behavior = structs.SessionKeysRelease
	case structs.SessionKeysRelease:
	case structs.SessionKeysDelete:
	default:
		return fmt.Errorf("Invalid Behavior setting '%s'", args.Session.Behavior)
	}

	if args.Session.TTL != "" {
		ttl, err := time.ParseDuration(args.Session.TTL)
		if err != nil {
			return fmt.Errorf("Session TTL '%s' invalid: %v", args.Session.TTL, err)
		}

		if ttl != 0 && (ttl < s.srv.config.SessionTTLMin || ttl > structs.SessionTTLMax) {
			return fmt.Errorf("Invalid Session TTL '%d', must be between [%v=%v]",
				ttl, s.srv.config.SessionTTLMin, structs.SessionTTLMax)
		}
	}

	if args.Op == structs.SessionCreate {

		state := s.srv.fsm.State()
		for {
			var err error
			if args.Session.ID, err = uuid.GenerateUUID(); err != nil {
				s.srv.logger.Printf("[ERR] consul.session: UUID generation failed: %v", err)
				return err
			}
			_, sess, err := state.SessionGet(nil, args.Session.ID)
			if err != nil {
				s.srv.logger.Printf("[ERR] consul.session: Session lookup failed: %v", err)
				return err
			}
			if sess == nil {
				break
			}
		}
	}

	resp, err := s.srv.raftApply(structs.SessionRequestType, args)
	if err != nil {
		s.srv.logger.Printf("[ERR] consul.session: Apply failed: %v", err)
		return err
	}

	if args.Op == structs.SessionCreate && args.Session.TTL != "" {

		s.srv.resetSessionTimer(args.Session.ID, &args.Session)
	} else if args.Op == structs.SessionDestroy {

		s.srv.clearSessionTimer(args.Session.ID)
	}

	if respErr, ok := resp.(error); ok {
		return respErr
	}

	if respString, ok := resp.(string); ok {
		*reply = respString
	}
	return nil
}

func (s *Session) Get(args *structs.SessionSpecificRequest,
	reply *structs.IndexedSessions) error {
	if done, err := s.srv.forward("Session.Get", args, args, reply); done {
		return err
	}

	return s.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, session, err := state.SessionGet(ws, args.Session)
			if err != nil {
				return err
			}

			reply.Index = index
			if session != nil {
				reply.Sessions = structs.Sessions{session}
			} else {
				reply.Sessions = nil
			}
			if err := s.srv.filterACL(args.Token, reply); err != nil {
				return err
			}
			return nil
		})
}

func (s *Session) List(args *structs.DCSpecificRequest,
	reply *structs.IndexedSessions) error {
	if done, err := s.srv.forward("Session.List", args, args, reply); done {
		return err
	}

	return s.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, sessions, err := state.SessionList(ws)
			if err != nil {
				return err
			}

			reply.Index, reply.Sessions = index, sessions
			if err := s.srv.filterACL(args.Token, reply); err != nil {
				return err
			}
			return nil
		})
}

func (s *Session) NodeSessions(args *structs.NodeSpecificRequest,
	reply *structs.IndexedSessions) error {
	if done, err := s.srv.forward("Session.NodeSessions", args, args, reply); done {
		return err
	}

	return s.srv.blockingQuery(
		&args.QueryOptions,
		&reply.QueryMeta,
		func(ws memdb.WatchSet, state *state.Store) error {
			index, sessions, err := state.NodeSessions(ws, args.Node)
			if err != nil {
				return err
			}

			reply.Index, reply.Sessions = index, sessions
			if err := s.srv.filterACL(args.Token, reply); err != nil {
				return err
			}
			return nil
		})
}

func (s *Session) Renew(args *structs.SessionSpecificRequest,
	reply *structs.IndexedSessions) error {
	if done, err := s.srv.forward("Session.Renew", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"consul", "session", "renew"}, time.Now())
	defer metrics.MeasureSince([]string{"session", "renew"}, time.Now())

	state := s.srv.fsm.State()
	index, session, err := state.SessionGet(nil, args.Session)
	if err != nil {
		return err
	}

	reply.Index = index
	if session == nil {
		return nil
	}

	rule, err := s.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	if rule != nil && s.srv.config.ACLEnforceVersion8 {
		if !rule.SessionWrite(session.Node) {
			return acl.ErrPermissionDenied
		}
	}

	reply.Sessions = structs.Sessions{session}
	if err := s.srv.resetSessionTimer(args.Session, session); err != nil {
		s.srv.logger.Printf("[ERR] consul.session: Session renew failed: %v", err)
		return err
	}

	return nil
}
