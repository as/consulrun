package watch

import (
	"context"
	"fmt"

	consulapi "github.com/as/consulrun/hashicorp/consul/api"
)

type watchFactory func(params map[string]interface{}) (WatcherFunc, error)

var watchFuncFactory map[string]watchFactory

func init() {
	watchFuncFactory = map[string]watchFactory{
		"key":       keyWatch,
		"keyprefix": keyPrefixWatch,
		"services":  servicesWatch,
		"nodes":     nodesWatch,
		"service":   serviceWatch,
		"checks":    checksWatch,
		"event":     eventWatch,
	}
}

func keyWatch(params map[string]interface{}) (WatcherFunc, error) {
	stale := false
	if err := assignValueBool(params, "stale", &stale); err != nil {
		return nil, err
	}

	var key string
	if err := assignValue(params, "key", &key); err != nil {
		return nil, err
	}
	if key == "" {
		return nil, fmt.Errorf("Must specify a single key to watch")
	}
	fn := func(p *Plan) (uint64, interface{}, error) {
		kv := p.client.KV()
		opts := makeQueryOptionsWithContext(p, stale)
		defer p.cancelFunc()
		pair, meta, err := kv.Get(key, &opts)
		if err != nil {
			return 0, nil, err
		}
		if pair == nil {
			return meta.LastIndex, nil, err
		}
		return meta.LastIndex, pair, err
	}
	return fn, nil
}

func keyPrefixWatch(params map[string]interface{}) (WatcherFunc, error) {
	stale := false
	if err := assignValueBool(params, "stale", &stale); err != nil {
		return nil, err
	}

	var prefix string
	if err := assignValue(params, "prefix", &prefix); err != nil {
		return nil, err
	}
	if prefix == "" {
		return nil, fmt.Errorf("Must specify a single prefix to watch")
	}
	fn := func(p *Plan) (uint64, interface{}, error) {
		kv := p.client.KV()
		opts := makeQueryOptionsWithContext(p, stale)
		defer p.cancelFunc()
		pairs, meta, err := kv.List(prefix, &opts)
		if err != nil {
			return 0, nil, err
		}
		return meta.LastIndex, pairs, err
	}
	return fn, nil
}

func servicesWatch(params map[string]interface{}) (WatcherFunc, error) {
	stale := false
	if err := assignValueBool(params, "stale", &stale); err != nil {
		return nil, err
	}

	fn := func(p *Plan) (uint64, interface{}, error) {
		catalog := p.client.Catalog()
		opts := makeQueryOptionsWithContext(p, stale)
		defer p.cancelFunc()
		services, meta, err := catalog.Services(&opts)
		if err != nil {
			return 0, nil, err
		}
		return meta.LastIndex, services, err
	}
	return fn, nil
}

func nodesWatch(params map[string]interface{}) (WatcherFunc, error) {
	stale := false
	if err := assignValueBool(params, "stale", &stale); err != nil {
		return nil, err
	}

	fn := func(p *Plan) (uint64, interface{}, error) {
		catalog := p.client.Catalog()
		opts := makeQueryOptionsWithContext(p, stale)
		defer p.cancelFunc()
		nodes, meta, err := catalog.Nodes(&opts)
		if err != nil {
			return 0, nil, err
		}
		return meta.LastIndex, nodes, err
	}
	return fn, nil
}

func serviceWatch(params map[string]interface{}) (WatcherFunc, error) {
	stale := false
	if err := assignValueBool(params, "stale", &stale); err != nil {
		return nil, err
	}

	var service, tag string
	if err := assignValue(params, "service", &service); err != nil {
		return nil, err
	}
	if service == "" {
		return nil, fmt.Errorf("Must specify a single service to watch")
	}

	if err := assignValue(params, "tag", &tag); err != nil {
		return nil, err
	}

	passingOnly := false
	if err := assignValueBool(params, "passingonly", &passingOnly); err != nil {
		return nil, err
	}

	fn := func(p *Plan) (uint64, interface{}, error) {
		health := p.client.Health()
		opts := makeQueryOptionsWithContext(p, stale)
		defer p.cancelFunc()
		nodes, meta, err := health.Service(service, tag, passingOnly, &opts)
		if err != nil {
			return 0, nil, err
		}
		return meta.LastIndex, nodes, err
	}
	return fn, nil
}

func checksWatch(params map[string]interface{}) (WatcherFunc, error) {
	stale := false
	if err := assignValueBool(params, "stale", &stale); err != nil {
		return nil, err
	}

	var service, state string
	if err := assignValue(params, "service", &service); err != nil {
		return nil, err
	}
	if err := assignValue(params, "state", &state); err != nil {
		return nil, err
	}
	if service != "" && state != "" {
		return nil, fmt.Errorf("Cannot specify service and state")
	}
	if service == "" && state == "" {
		state = "any"
	}

	fn := func(p *Plan) (uint64, interface{}, error) {
		health := p.client.Health()
		opts := makeQueryOptionsWithContext(p, stale)
		defer p.cancelFunc()
		var checks []*consulapi.HealthCheck
		var meta *consulapi.QueryMeta
		var err error
		if state != "" {
			checks, meta, err = health.State(state, &opts)
		} else {
			checks, meta, err = health.Checks(service, &opts)
		}
		if err != nil {
			return 0, nil, err
		}
		return meta.LastIndex, checks, err
	}
	return fn, nil
}

func eventWatch(params map[string]interface{}) (WatcherFunc, error) {

	var name string
	if err := assignValue(params, "name", &name); err != nil {
		return nil, err
	}

	fn := func(p *Plan) (uint64, interface{}, error) {
		event := p.client.Event()
		opts := makeQueryOptionsWithContext(p, false)
		defer p.cancelFunc()
		events, meta, err := event.List(name, &opts)
		if err != nil {
			return 0, nil, err
		}

		for i := 0; i < len(events); i++ {
			if event.IDToIndex(events[i].ID) == p.lastIndex {
				events = events[i+1:]
				break
			}
		}
		return meta.LastIndex, events, err
	}
	return fn, nil
}

func makeQueryOptionsWithContext(p *Plan, stale bool) consulapi.QueryOptions {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancelFunc = cancel
	opts := consulapi.QueryOptions{AllowStale: stale, WaitIndex: p.lastIndex}
	return *opts.WithContext(ctx)
}
