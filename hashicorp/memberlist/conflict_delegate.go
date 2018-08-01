package memberlist

type ConflictDelegate interface {
	NotifyConflict(existing, other *Node)
}
