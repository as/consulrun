package memberlist

type MergeDelegate interface {
	NotifyMerge(peers []*Node) error
}
