package memberlist

type Delegate interface {
	NodeMeta(limit int) []byte

	NotifyMsg([]byte)

	GetBroadcasts(overhead, limit int) [][]byte

	LocalState(join bool) []byte

	MergeRemoteState(buf []byte, join bool)
}
