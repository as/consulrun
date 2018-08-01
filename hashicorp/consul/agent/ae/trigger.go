package ae

type Trigger struct {
	ch chan struct{}
}

func NewTrigger() *Trigger {
	return &Trigger{make(chan struct{}, 1)}
}

func (t Trigger) Trigger() {
	select {
	case t.ch <- struct{}{}:
	default:
	}
}

func (t Trigger) Notif() <-chan struct{} {
	return t.ch
}
