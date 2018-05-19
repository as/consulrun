package consul

import (
	"fmt"
	"net"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/metadata"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
)

func (op *Operator) RaftGetConfiguration(args *structs.DCSpecificRequest, reply *structs.RaftConfigurationResponse) error {
	if done, err := op.srv.forward("Operator.RaftGetConfiguration", args, args, reply); done {
		return err
	}

	rule, err := op.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	if rule != nil && !rule.OperatorRead() {
		return acl.ErrPermissionDenied
	}

	future := op.srv.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return err
	}

	serverMap := make(map[raft.ServerAddress]serf.Member)
	for _, member := range op.srv.serfLAN.Members() {
		valid, parts := metadata.IsConsulServer(member)
		if !valid {
			continue
		}

		addr := (&net.TCPAddr{IP: member.Addr, Port: parts.Port}).String()
		serverMap[raft.ServerAddress(addr)] = member
	}

	leader := op.srv.raft.Leader()
	reply.Index = future.Index()
	for _, server := range future.Configuration().Servers {
		node := "(unknown)"
		raftProtocolVersion := "unknown"
		if member, ok := serverMap[server.Address]; ok {
			node = member.Name
			raftProtocolVersion = member.Tags["raft_vsn"]
		}

		entry := &structs.RaftServer{
			ID:              server.ID,
			Node:            node,
			Address:         server.Address,
			Leader:          server.Address == leader,
			Voter:           server.Suffrage == raft.Voter,
			ProtocolVersion: raftProtocolVersion,
		}
		reply.Servers = append(reply.Servers, entry)
	}
	return nil
}

func (op *Operator) RaftRemovePeerByAddress(args *structs.RaftRemovePeerRequest, reply *struct{}) error {
	if done, err := op.srv.forward("Operator.RaftRemovePeerByAddress", args, args, reply); done {
		return err
	}

	rule, err := op.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	if rule != nil && !rule.OperatorWrite() {
		return acl.ErrPermissionDenied
	}

	{
		future := op.srv.raft.GetConfiguration()
		if err := future.Error(); err != nil {
			return err
		}
		for _, s := range future.Configuration().Servers {
			if s.Address == args.Address {
				args.ID = s.ID
				goto REMOVE
			}
		}
		return fmt.Errorf("address %q was not found in the Raft configuration",
			args.Address)
	}

REMOVE:

	minRaftProtocol, err := op.srv.autopilot.MinRaftProtocol()
	if err != nil {
		return err
	}

	var future raft.Future
	if minRaftProtocol >= 2 {
		future = op.srv.raft.RemoveServer(args.ID, 0, 0)
	} else {
		future = op.srv.raft.RemovePeer(args.Address)
	}
	if err := future.Error(); err != nil {
		op.srv.logger.Printf("[WARN] consul.operator: Failed to remove Raft peer %q: %v",
			args.Address, err)
		return err
	}

	op.srv.logger.Printf("[WARN] consul.operator: Removed Raft peer %q", args.Address)
	return nil
}

func (op *Operator) RaftRemovePeerByID(args *structs.RaftRemovePeerRequest, reply *struct{}) error {
	if done, err := op.srv.forward("Operator.RaftRemovePeerByID", args, args, reply); done {
		return err
	}

	rule, err := op.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	if rule != nil && !rule.OperatorWrite() {
		return acl.ErrPermissionDenied
	}

	{
		future := op.srv.raft.GetConfiguration()
		if err := future.Error(); err != nil {
			return err
		}
		for _, s := range future.Configuration().Servers {
			if s.ID == args.ID {
				args.Address = s.Address
				goto REMOVE
			}
		}
		return fmt.Errorf("id %q was not found in the Raft configuration",
			args.ID)
	}

REMOVE:

	minRaftProtocol, err := op.srv.autopilot.MinRaftProtocol()
	if err != nil {
		return err
	}

	var future raft.Future
	if minRaftProtocol >= 2 {
		future = op.srv.raft.RemoveServer(args.ID, 0, 0)
	} else {
		future = op.srv.raft.RemovePeer(args.Address)
	}
	if err := future.Error(); err != nil {
		op.srv.logger.Printf("[WARN] consul.operator: Failed to remove Raft peer with id %q: %v",
			args.ID, err)
		return err
	}

	op.srv.logger.Printf("[WARN] consul.operator: Removed Raft peer with id %q", args.ID)
	return nil
}
