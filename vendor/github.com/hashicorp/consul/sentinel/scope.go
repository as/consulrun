package sentinel

import (
	"github.com/hashicorp/consul/api"
)

type ScopeFn func() map[string]interface{}

func ScopeKVUpsert(key string, value []byte, flags uint64) map[string]interface{} {
	return map[string]interface{}{
		"key":   key,
		"value": string(value),
		"flags": flags,
	}
}

func ScopeCatalogUpsert(node *api.Node, service *api.AgentService) map[string]interface{} {
	return map[string]interface{}{
		"node":    node,
		"service": service,
	}
}
