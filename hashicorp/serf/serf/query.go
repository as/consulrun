package serf

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"regexp"
	"sync"
	"time"
)

type QueryParam struct {
	FilterNodes []string

	FilterTags map[string]string

	RequestAck bool

	RelayFactor uint8

	Timeout time.Duration
}

func (s *Serf) DefaultQueryTimeout() time.Duration {
	n := s.memberlist.NumMembers()
	timeout := s.config.MemberlistConfig.GossipInterval
	timeout *= time.Duration(s.config.QueryTimeoutMult)
	timeout *= time.Duration(math.Ceil(math.Log10(float64(n + 1))))
	return timeout
}

func (s *Serf) DefaultQueryParams() *QueryParam {
	return &QueryParam{
		FilterNodes: nil,
		FilterTags:  nil,
		RequestAck:  false,
		Timeout:     s.DefaultQueryTimeout(),
	}
}

func (q *QueryParam) encodeFilters() ([][]byte, error) {
	var filters [][]byte

	if len(q.FilterNodes) > 0 {
		if buf, err := encodeFilter(filterNodeType, q.FilterNodes); err != nil {
			return nil, err
		} else {
			filters = append(filters, buf)
		}
	}

	for tag, expr := range q.FilterTags {
		filt := filterTag{tag, expr}
		if buf, err := encodeFilter(filterTagType, &filt); err != nil {
			return nil, err
		} else {
			filters = append(filters, buf)
		}
	}

	return filters, nil
}

type QueryResponse struct {
	ackCh chan string

	deadline time.Time

	id uint32

	lTime LamportTime

	respCh chan NodeResponse

	acks      map[string]struct{}
	responses map[string]struct{}

	closed    bool
	closeLock sync.Mutex
}

func newQueryResponse(n int, q *messageQuery) *QueryResponse {
	resp := &QueryResponse{
		deadline:  time.Now().Add(q.Timeout),
		id:        q.ID,
		lTime:     q.LTime,
		respCh:    make(chan NodeResponse, n),
		responses: make(map[string]struct{}),
	}
	if q.Ack() {
		resp.ackCh = make(chan string, n)
		resp.acks = make(map[string]struct{})
	}
	return resp
}

func (r *QueryResponse) Close() {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	if r.ackCh != nil {
		close(r.ackCh)
	}
	if r.respCh != nil {
		close(r.respCh)
	}
}

func (r *QueryResponse) Deadline() time.Time {
	return r.deadline
}

func (r *QueryResponse) Finished() bool {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()
	return r.closed || time.Now().After(r.deadline)
}

func (r *QueryResponse) AckCh() <-chan string {
	return r.ackCh
}

func (r *QueryResponse) ResponseCh() <-chan NodeResponse {
	return r.respCh
}

func (r *QueryResponse) sendResponse(nr NodeResponse) error {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()
	if r.closed {
		return nil
	}
	select {
	case r.respCh <- nr:
		r.responses[nr.From] = struct{}{}
	default:
		return errors.New("serf: Failed to deliver query response, dropping")
	}
	return nil
}

type NodeResponse struct {
	From    string
	Payload []byte
}

func (s *Serf) shouldProcessQuery(filters [][]byte) bool {
	for _, filter := range filters {
		switch filterType(filter[0]) {
		case filterNodeType:

			var nodes filterNode
			if err := decodeMessage(filter[1:], &nodes); err != nil {
				s.logger.Printf("[WARN] serf: failed to decode filterNodeType: %v", err)
				return false
			}

			found := false
			for _, n := range nodes {
				if n == s.config.NodeName {
					found = true
					break
				}
			}
			if !found {
				return false
			}

		case filterTagType:

			var filt filterTag
			if err := decodeMessage(filter[1:], &filt); err != nil {
				s.logger.Printf("[WARN] serf: failed to decode filterTagType: %v", err)
				return false
			}

			tags := s.config.Tags
			matched, err := regexp.MatchString(filt.Expr, tags[filt.Tag])
			if err != nil {
				s.logger.Printf("[WARN] serf: failed to compile filter regex (%s): %v", filt.Expr, err)
				return false
			}
			if !matched {
				return false
			}

		default:
			s.logger.Printf("[WARN] serf: query has unrecognized filter type: %d", filter[0])
			return false
		}
	}
	return true
}

func (s *Serf) relayResponse(relayFactor uint8, addr net.UDPAddr, resp *messageQueryResponse) error {
	if relayFactor == 0 {
		return nil
	}

	members := s.Members()
	if len(members) < int(relayFactor)+1 {
		return nil
	}

	raw, err := encodeRelayMessage(messageQueryResponseType, addr, &resp)
	if err != nil {
		return fmt.Errorf("failed to format relayed response: %v", err)
	}
	if len(raw) > s.config.QueryResponseSizeLimit {
		return fmt.Errorf("relayed response exceeds limit of %d bytes", s.config.QueryResponseSizeLimit)
	}

	localName := s.LocalMember().Name
	relayMembers := kRandomMembers(int(relayFactor), members, func(m Member) bool {
		return m.Status != StatusAlive || m.ProtocolMax < 5 || m.Name == localName
	})
	for _, m := range relayMembers {
		relayAddr := net.UDPAddr{IP: m.Addr, Port: int(m.Port)}
		if err := s.memberlist.SendTo(&relayAddr, raw); err != nil {
			return fmt.Errorf("failed to send relay response: %v", err)
		}
	}
	return nil
}

func kRandomMembers(k int, members []Member, filterFunc func(Member) bool) []Member {
	n := len(members)
	kMembers := make([]Member, 0, k)
OUTER:

	for i := 0; i < 3*n && len(kMembers) < k; i++ {

		idx := rand.Intn(n)
		member := members[idx]

		if filterFunc != nil && filterFunc(member) {
			continue OUTER
		}

		for j := 0; j < len(kMembers); j++ {
			if member.Name == kMembers[j].Name {
				continue OUTER
			}
		}

		kMembers = append(kMembers, member)
	}

	return kMembers
}
