package raft

import (
	"fmt"
	"io"
	"sync"
	"time"
)

func NewInmemAddr() ServerAddress {
	return ServerAddress(generateUUID())
}

type inmemPipeline struct {
	trans    *InmemTransport
	peer     *InmemTransport
	peerAddr ServerAddress

	doneCh       chan AppendFuture
	inprogressCh chan *inmemPipelineInflight

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

type inmemPipelineInflight struct {
	future *appendFuture
	respCh <-chan RPCResponse
}

type InmemTransport struct {
	sync.RWMutex
	consumerCh chan RPC
	localAddr  ServerAddress
	peers      map[ServerAddress]*InmemTransport
	pipelines  []*inmemPipeline
	timeout    time.Duration
}

func NewInmemTransport(addr ServerAddress) (ServerAddress, *InmemTransport) {
	if string(addr) == "" {
		addr = NewInmemAddr()
	}
	trans := &InmemTransport{
		consumerCh: make(chan RPC, 16),
		localAddr:  addr,
		peers:      make(map[ServerAddress]*InmemTransport),
		timeout:    50 * time.Millisecond,
	}
	return addr, trans
}

func (i *InmemTransport) SetHeartbeatHandler(cb func(RPC)) {
}

func (i *InmemTransport) Consumer() <-chan RPC {
	return i.consumerCh
}

func (i *InmemTransport) LocalAddr() ServerAddress {
	return i.localAddr
}

func (i *InmemTransport) AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error) {
	i.RLock()
	peer, ok := i.peers[target]
	i.RUnlock()
	if !ok {
		return nil, fmt.Errorf("failed to connect to peer: %v", target)
	}
	pipeline := newInmemPipeline(i, peer, target)
	i.Lock()
	i.pipelines = append(i.pipelines, pipeline)
	i.Unlock()
	return pipeline, nil
}

func (i *InmemTransport) AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, i.timeout)
	if err != nil {
		return err
	}

	out := rpcResp.Response.(*AppendEntriesResponse)
	*resp = *out
	return nil
}

func (i *InmemTransport) RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error {
	rpcResp, err := i.makeRPC(target, args, nil, i.timeout)
	if err != nil {
		return err
	}

	out := rpcResp.Response.(*RequestVoteResponse)
	*resp = *out
	return nil
}

func (i *InmemTransport) InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error {
	rpcResp, err := i.makeRPC(target, args, data, 10*i.timeout)
	if err != nil {
		return err
	}

	out := rpcResp.Response.(*InstallSnapshotResponse)
	*resp = *out
	return nil
}

func (i *InmemTransport) makeRPC(target ServerAddress, args interface{}, r io.Reader, timeout time.Duration) (rpcResp RPCResponse, err error) {
	i.RLock()
	peer, ok := i.peers[target]
	i.RUnlock()

	if !ok {
		err = fmt.Errorf("failed to connect to peer: %v", target)
		return
	}

	respCh := make(chan RPCResponse)
	peer.consumerCh <- RPC{
		Command:  args,
		Reader:   r,
		RespChan: respCh,
	}

	select {
	case rpcResp = <-respCh:
		if rpcResp.Error != nil {
			err = rpcResp.Error
		}
	case <-time.After(timeout):
		err = fmt.Errorf("command timed out")
	}
	return
}

func (i *InmemTransport) EncodePeer(id ServerID, p ServerAddress) []byte {
	return []byte(p)
}

func (i *InmemTransport) DecodePeer(buf []byte) ServerAddress {
	return ServerAddress(buf)
}

func (i *InmemTransport) Connect(peer ServerAddress, t Transport) {
	trans := t.(*InmemTransport)
	i.Lock()
	defer i.Unlock()
	i.peers[peer] = trans
}

func (i *InmemTransport) Disconnect(peer ServerAddress) {
	i.Lock()
	defer i.Unlock()
	delete(i.peers, peer)

	n := len(i.pipelines)
	for idx := 0; idx < n; idx++ {
		if i.pipelines[idx].peerAddr == peer {
			i.pipelines[idx].Close()
			i.pipelines[idx], i.pipelines[n-1] = i.pipelines[n-1], nil
			idx--
			n--
		}
	}
	i.pipelines = i.pipelines[:n]
}

func (i *InmemTransport) DisconnectAll() {
	i.Lock()
	defer i.Unlock()
	i.peers = make(map[ServerAddress]*InmemTransport)

	for _, pipeline := range i.pipelines {
		pipeline.Close()
	}
	i.pipelines = nil
}

func (i *InmemTransport) Close() error {
	i.DisconnectAll()
	return nil
}

func newInmemPipeline(trans *InmemTransport, peer *InmemTransport, addr ServerAddress) *inmemPipeline {
	i := &inmemPipeline{
		trans:        trans,
		peer:         peer,
		peerAddr:     addr,
		doneCh:       make(chan AppendFuture, 16),
		inprogressCh: make(chan *inmemPipelineInflight, 16),
		shutdownCh:   make(chan struct{}),
	}
	go i.decodeResponses()
	return i
}

func (i *inmemPipeline) decodeResponses() {
	timeout := i.trans.timeout
	for {
		select {
		case inp := <-i.inprogressCh:
			var timeoutCh <-chan time.Time
			if timeout > 0 {
				timeoutCh = time.After(timeout)
			}

			select {
			case rpcResp := <-inp.respCh:

				*inp.future.resp = *rpcResp.Response.(*AppendEntriesResponse)
				inp.future.respond(rpcResp.Error)

				select {
				case i.doneCh <- inp.future:
				case <-i.shutdownCh:
					return
				}

			case <-timeoutCh:
				inp.future.respond(fmt.Errorf("command timed out"))
				select {
				case i.doneCh <- inp.future:
				case <-i.shutdownCh:
					return
				}

			case <-i.shutdownCh:
				return
			}
		case <-i.shutdownCh:
			return
		}
	}
}

func (i *inmemPipeline) AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error) {

	future := &appendFuture{
		start: time.Now(),
		args:  args,
		resp:  resp,
	}
	future.init()

	var timeout <-chan time.Time
	if i.trans.timeout > 0 {
		timeout = time.After(i.trans.timeout)
	}

	respCh := make(chan RPCResponse, 1)
	rpc := RPC{
		Command:  args,
		RespChan: respCh,
	}
	select {
	case i.peer.consumerCh <- rpc:
	case <-timeout:
		return nil, fmt.Errorf("command enqueue timeout")
	case <-i.shutdownCh:
		return nil, ErrPipelineShutdown
	}

	select {
	case i.inprogressCh <- &inmemPipelineInflight{future, respCh}:
		return future, nil
	case <-i.shutdownCh:
		return nil, ErrPipelineShutdown
	}
}

func (i *inmemPipeline) Consumer() <-chan AppendFuture {
	return i.doneCh
}

func (i *inmemPipeline) Close() error {
	i.shutdownLock.Lock()
	defer i.shutdownLock.Unlock()
	if i.shutdown {
		return nil
	}

	i.shutdown = true
	close(i.shutdownCh)
	return nil
}
