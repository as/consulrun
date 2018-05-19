package agent

import (
	"fmt"

	"github.com/hashicorp/consul/agent/structs"
)

func (a *Agent) TranslateAddress(dc string, addr string, taggedAddresses map[string]string) string {
	if a.config.TranslateWANAddrs && (a.config.Datacenter != dc) {
		wanAddr := taggedAddresses["wan"]
		if wanAddr != "" {
			addr = wanAddr
		}
	}
	return addr
}

func (a *Agent) TranslateAddresses(dc string, subj interface{}) {

	if !a.config.TranslateWANAddrs || (a.config.Datacenter == dc) {
		return
	}

	switch v := subj.(type) {
	case structs.CheckServiceNodes:
		for _, entry := range v {
			entry.Node.Address = a.TranslateAddress(dc, entry.Node.Address, entry.Node.TaggedAddresses)
		}
	case *structs.Node:
		v.Address = a.TranslateAddress(dc, v.Address, v.TaggedAddresses)
	case structs.Nodes:
		for _, node := range v {
			node.Address = a.TranslateAddress(dc, node.Address, node.TaggedAddresses)
		}
	case structs.ServiceNodes:
		for _, entry := range v {
			entry.Address = a.TranslateAddress(dc, entry.Address, entry.TaggedAddresses)
		}
	default:
		panic(fmt.Errorf("Unhandled type passed to address translator: %#v", subj))
	}
}
