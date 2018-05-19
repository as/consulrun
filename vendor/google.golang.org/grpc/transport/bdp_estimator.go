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

package transport

import (
	"sync"
	"time"
)

const (
	bdpLimit = (1 << 20) * 4

	alpha = 0.9

	beta = 0.66

	gamma = 2
)

var bdpPing = &ping{data: [8]byte{2, 4, 16, 16, 9, 14, 7, 7}}

type bdpEstimator struct {
	sentAt time.Time

	mu sync.Mutex

	bdp uint32

	sample uint32

	bwMax float64

	isSent bool

	updateFlowControl func(n uint32)

	sampleCount uint64

	rtt float64
}

func (b *bdpEstimator) timesnap(d [8]byte) {
	if bdpPing.data != d {
		return
	}
	b.sentAt = time.Now()
}

func (b *bdpEstimator) add(n uint32) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.bdp == bdpLimit {
		return false
	}
	if !b.isSent {
		b.isSent = true
		b.sample = n
		b.sentAt = time.Time{}
		b.sampleCount++
		return true
	}
	b.sample += n
	return false
}

func (b *bdpEstimator) calculate(d [8]byte) {

	if bdpPing.data != d {
		return
	}
	b.mu.Lock()
	rttSample := time.Since(b.sentAt).Seconds()
	if b.sampleCount < 10 {

		b.rtt += (rttSample - b.rtt) / float64(b.sampleCount)
	} else {

		b.rtt += (rttSample - b.rtt) * float64(alpha)
	}
	b.isSent = false

	bwCurrent := float64(b.sample) / (b.rtt * float64(1.5))
	if bwCurrent > b.bwMax {
		b.bwMax = bwCurrent
	}

	if float64(b.sample) >= beta*float64(b.bdp) && bwCurrent == b.bwMax && b.bdp != bdpLimit {
		sampleFloat := float64(b.sample)
		b.bdp = uint32(gamma * sampleFloat)
		if b.bdp > bdpLimit {
			b.bdp = bdpLimit
		}
		bdp := b.bdp
		b.mu.Unlock()
		b.updateFlowControl(bdp)
		return
	}
	b.mu.Unlock()
}
