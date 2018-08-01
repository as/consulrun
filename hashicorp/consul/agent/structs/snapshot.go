package structs

type SnapshotOp int

const (
	SnapshotSave SnapshotOp = iota
	SnapshotRestore
)

type SnapshotReplyFn func(reply *SnapshotResponse) error

type SnapshotRequest struct {
	Datacenter string

	Token string

	AllowStale bool

	Op SnapshotOp
}

type SnapshotResponse struct {
	Error string

	QueryMeta
}
