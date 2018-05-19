/*
 *
 * Copyright 2017 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package grpc

import (
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/transport"
)

type pickerWrapper struct {
	mu         sync.Mutex
	done       bool
	blockingCh chan struct{}
	picker     balancer.Picker
}

func newPickerWrapper() *pickerWrapper {
	bp := &pickerWrapper{blockingCh: make(chan struct{})}
	return bp
}

func (bp *pickerWrapper) updatePicker(p balancer.Picker) {
	bp.mu.Lock()
	if bp.done {
		bp.mu.Unlock()
		return
	}
	bp.picker = p

	close(bp.blockingCh)
	bp.blockingCh = make(chan struct{})
	bp.mu.Unlock()
}

func (bp *pickerWrapper) pick(ctx context.Context, failfast bool, opts balancer.PickOptions) (transport.ClientTransport, func(balancer.DoneInfo), error) {
	var (
		p  balancer.Picker
		ch chan struct{}
	)

	for {
		bp.mu.Lock()
		if bp.done {
			bp.mu.Unlock()
			return nil, nil, ErrClientConnClosing
		}

		if bp.picker == nil {
			ch = bp.blockingCh
		}
		if ch == bp.blockingCh {

			bp.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-ch:
			}
			continue
		}

		ch = bp.blockingCh
		p = bp.picker
		bp.mu.Unlock()

		subConn, done, err := p.Pick(ctx, opts)

		if err != nil {
			switch err {
			case balancer.ErrNoSubConnAvailable:
				continue
			case balancer.ErrTransientFailure:
				if !failfast {
					continue
				}
				return nil, nil, status.Errorf(codes.Unavailable, "%v", err)
			default:

				return nil, nil, toRPCErr(err)
			}
		}

		acw, ok := subConn.(*acBalancerWrapper)
		if !ok {
			grpclog.Infof("subconn returned from pick is not *acBalancerWrapper")
			continue
		}
		if t, ok := acw.getAddrConn().getReadyTransport(); ok {
			return t, done, nil
		}
		grpclog.Infof("blockingPicker: the picked transport is not ready, loop back to repick")

	}
}

func (bp *pickerWrapper) close() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if bp.done {
		return
	}
	bp.done = true
	close(bp.blockingCh)
}
