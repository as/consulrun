package watch

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	consulapi "github.com/hashicorp/consul/api"
)

const (
	retryInterval = 5 * time.Second

	maxBackoffTime = 180 * time.Second
)

func (p *Plan) Run(address string) error {

	p.address = address
	conf := consulapi.DefaultConfig()
	conf.Address = address
	conf.Datacenter = p.Datacenter
	conf.Token = p.Token
	client, err := consulapi.NewClient(conf)
	if err != nil {
		return fmt.Errorf("Failed to connect to agent: %v", err)
	}
	p.client = client

	output := p.LogOutput
	if output == nil {
		output = os.Stderr
	}
	logger := log.New(output, "", log.LstdFlags)

	failures := 0
OUTER:
	for !p.shouldStop() {

		index, result, err := p.Watcher(p)

		if p.shouldStop() {
			break
		}

		if err != nil {

			failures++
			p.lastIndex = 0
			retry := retryInterval * time.Duration(failures*failures)
			if retry > maxBackoffTime {
				retry = maxBackoffTime
			}
			logger.Printf("[ERR] consul.watch: Watch (type: %s) errored: %v, retry in %v",
				p.Type, err, retry)
			select {
			case <-time.After(retry):
				continue OUTER
			case <-p.stopCh:
				return nil
			}
		}

		failures = 0

		if index == p.lastIndex {
			continue
		}

		oldIndex := p.lastIndex
		p.lastIndex = index
		if oldIndex != 0 && reflect.DeepEqual(p.lastResult, result) {
			continue
		}
		if p.lastIndex < oldIndex {
			p.lastIndex = 0
		}

		p.lastResult = result
		if p.Handler != nil {
			p.Handler(index, result)
		}
	}
	return nil
}

func (p *Plan) Stop() {
	p.stopLock.Lock()
	defer p.stopLock.Unlock()
	if p.stop {
		return
	}
	p.stop = true
	if p.cancelFunc != nil {
		p.cancelFunc()
	}
	close(p.stopCh)
}

func (p *Plan) shouldStop() bool {
	select {
	case <-p.stopCh:
		return true
	default:
		return false
	}
}

func (p *Plan) IsStopped() bool {
	p.stopLock.Lock()
	defer p.stopLock.Unlock()
	return p.stop
}
