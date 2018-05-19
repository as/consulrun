package memberlist

type AliveDelegate interface {
	NotifyAlive(peer *Node) error
}
