package memberlist

import (
	"sort"
	"sync"
)

type TransmitLimitedQueue struct {
	NumNodes func() int

	RetransmitMult int

	sync.Mutex
	bcQueue limitedBroadcasts
}

type limitedBroadcast struct {
	transmits int // Number of transmissions attempted.
	b         Broadcast
}
type limitedBroadcasts []*limitedBroadcast

type Broadcast interface {
	Invalidates(b Broadcast) bool

	Message() []byte

	Finished()
}

func (q *TransmitLimitedQueue) QueueBroadcast(b Broadcast) {
	q.Lock()
	defer q.Unlock()

	n := len(q.bcQueue)
	for i := 0; i < n; i++ {
		if b.Invalidates(q.bcQueue[i].b) {
			q.bcQueue[i].b.Finished()
			copy(q.bcQueue[i:], q.bcQueue[i+1:])
			q.bcQueue[n-1] = nil
			q.bcQueue = q.bcQueue[:n-1]
			n--
		}
	}

	q.bcQueue = append(q.bcQueue, &limitedBroadcast{0, b})
}

func (q *TransmitLimitedQueue) GetBroadcasts(overhead, limit int) [][]byte {
	q.Lock()
	defer q.Unlock()

	if len(q.bcQueue) == 0 {
		return nil
	}

	transmitLimit := retransmitLimit(q.RetransmitMult, q.NumNodes())
	bytesUsed := 0
	var toSend [][]byte

	for i := len(q.bcQueue) - 1; i >= 0; i-- {

		b := q.bcQueue[i]
		msg := b.b.Message()
		if bytesUsed+overhead+len(msg) > limit {
			continue
		}

		bytesUsed += overhead + len(msg)
		toSend = append(toSend, msg)

		b.transmits++
		if b.transmits >= transmitLimit {
			b.b.Finished()
			n := len(q.bcQueue)
			q.bcQueue[i], q.bcQueue[n-1] = q.bcQueue[n-1], nil
			q.bcQueue = q.bcQueue[:n-1]
		}
	}

	if len(toSend) > 0 {
		q.bcQueue.Sort()
	}
	return toSend
}

func (q *TransmitLimitedQueue) NumQueued() int {
	q.Lock()
	defer q.Unlock()
	return len(q.bcQueue)
}

func (q *TransmitLimitedQueue) Reset() {
	q.Lock()
	defer q.Unlock()
	for _, b := range q.bcQueue {
		b.b.Finished()
	}
	q.bcQueue = nil
}

func (q *TransmitLimitedQueue) Prune(maxRetain int) {
	q.Lock()
	defer q.Unlock()

	n := len(q.bcQueue)
	if n < maxRetain {
		return
	}

	for i := 0; i < n-maxRetain; i++ {
		q.bcQueue[i].b.Finished()
	}

	copy(q.bcQueue[0:], q.bcQueue[n-maxRetain:])
	q.bcQueue = q.bcQueue[:maxRetain]
}

func (b limitedBroadcasts) Len() int {
	return len(b)
}

func (b limitedBroadcasts) Less(i, j int) bool {
	return b[i].transmits < b[j].transmits
}

func (b limitedBroadcasts) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b limitedBroadcasts) Sort() {
	sort.Sort(sort.Reverse(b))
}
