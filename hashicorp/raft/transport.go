package raft

import (
	"io"
	"time"
)

type RPCResponse struct {
	Response interface{}
	Error    error
}

type RPC struct {
	Command  interface{}
	Reader   io.Reader // Set only for InstallSnapshot
	RespChan chan<- RPCResponse
}

func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- RPCResponse{resp, err}
}

type Transport interface {
	Consumer() <-chan RPC

	LocalAddr() ServerAddress

	AppendEntriesPipeline(id ServerID, target ServerAddress) (AppendPipeline, error)

	AppendEntries(id ServerID, target ServerAddress, args *AppendEntriesRequest, resp *AppendEntriesResponse) error

	RequestVote(id ServerID, target ServerAddress, args *RequestVoteRequest, resp *RequestVoteResponse) error

	InstallSnapshot(id ServerID, target ServerAddress, args *InstallSnapshotRequest, resp *InstallSnapshotResponse, data io.Reader) error

	EncodePeer(id ServerID, addr ServerAddress) []byte

	DecodePeer([]byte) ServerAddress

	SetHeartbeatHandler(cb func(rpc RPC))
}

//
type WithClose interface {
	Close() error
}

type LoopbackTransport interface {
	Transport // Embedded transport reference
	WithPeers // Embedded peer management
	WithClose // with a close routine
}

type WithPeers interface {
	Connect(peer ServerAddress, t Transport) // Connect a peer
	Disconnect(peer ServerAddress)           // Disconnect a given peer
	DisconnectAll()                          // Disconnect all peers, possibly to reconnect them later
}

type AppendPipeline interface {
	AppendEntries(args *AppendEntriesRequest, resp *AppendEntriesResponse) (AppendFuture, error)

	Consumer() <-chan AppendFuture

	Close() error
}

type AppendFuture interface {
	Future

	Start() time.Time

	Request() *AppendEntriesRequest

	Response() *AppendEntriesResponse
}
