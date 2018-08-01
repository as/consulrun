package raft

type RPCHeader struct {
	ProtocolVersion ProtocolVersion
}

type WithRPCHeader interface {
	GetRPCHeader() RPCHeader
}

type AppendEntriesRequest struct {
	RPCHeader

	Term   uint64
	Leader []byte

	PrevLogEntry uint64
	PrevLogTerm  uint64

	Entries []*Log

	LeaderCommitIndex uint64
}

func (r *AppendEntriesRequest) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

type AppendEntriesResponse struct {
	RPCHeader

	Term uint64

	LastLog uint64

	Success bool

	NoRetryBackoff bool
}

func (r *AppendEntriesResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

type RequestVoteRequest struct {
	RPCHeader

	Term      uint64
	Candidate []byte

	LastLogIndex uint64
	LastLogTerm  uint64
}

func (r *RequestVoteRequest) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

type RequestVoteResponse struct {
	RPCHeader

	Term uint64

	Peers []byte

	Granted bool
}

func (r *RequestVoteResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

type InstallSnapshotRequest struct {
	RPCHeader
	SnapshotVersion SnapshotVersion

	Term   uint64
	Leader []byte

	LastLogIndex uint64
	LastLogTerm  uint64

	Peers []byte

	Configuration []byte

	ConfigurationIndex uint64

	Size int64
}

func (r *InstallSnapshotRequest) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

type InstallSnapshotResponse struct {
	RPCHeader

	Term    uint64
	Success bool
}

func (r *InstallSnapshotResponse) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}
