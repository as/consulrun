package consul

import (
	"fmt"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/consul/autopilot"
	"github.com/hashicorp/consul/agent/structs"
)

func (op *Operator) AutopilotGetConfiguration(args *structs.DCSpecificRequest, reply *autopilot.Config) error {
	if done, err := op.srv.forward("Operator.AutopilotGetConfiguration", args, args, reply); done {
		return err
	}

	rule, err := op.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	if rule != nil && !rule.OperatorRead() {
		return acl.ErrPermissionDenied
	}

	state := op.srv.fsm.State()
	_, config, err := state.AutopilotConfig()
	if err != nil {
		return err
	}
	if config == nil {
		return fmt.Errorf("autopilot config not initialized yet")
	}

	*reply = *config

	return nil
}

func (op *Operator) AutopilotSetConfiguration(args *structs.AutopilotSetConfigRequest, reply *bool) error {
	if done, err := op.srv.forward("Operator.AutopilotSetConfiguration", args, args, reply); done {
		return err
	}

	rule, err := op.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	if rule != nil && !rule.OperatorWrite() {
		return acl.ErrPermissionDenied
	}

	resp, err := op.srv.raftApply(structs.AutopilotRequestType, args)
	if err != nil {
		op.srv.logger.Printf("[ERR] consul.operator: Apply failed: %v", err)
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

func (op *Operator) ServerHealth(args *structs.DCSpecificRequest, reply *autopilot.OperatorHealthReply) error {

	args.RequireConsistent = true
	args.AllowStale = false
	if done, err := op.srv.forward("Operator.ServerHealth", args, args, reply); done {
		return err
	}

	rule, err := op.srv.resolveToken(args.Token)
	if err != nil {
		return err
	}
	if rule != nil && !rule.OperatorRead() {
		return acl.ErrPermissionDenied
	}

	minRaftProtocol, err := op.srv.autopilot.MinRaftProtocol()
	if err != nil {
		return fmt.Errorf("error getting server raft protocol versions: %s", err)
	}
	if minRaftProtocol < 3 {
		return fmt.Errorf("all servers must have raft_protocol set to 3 or higher to use this endpoint")
	}

	*reply = op.srv.autopilot.GetClusterHealth()

	return nil
}
