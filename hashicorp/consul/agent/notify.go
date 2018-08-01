package agent

import (
	"sync"
)

type NotifyGroup struct {
	l      sync.Mutex
	notify map[chan struct{}]struct{}
}

func (n *NotifyGroup) Notify() {
	n.l.Lock()
	defer n.l.Unlock()
	for ch := range n.notify {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	n.notify = nil
}

func (n *NotifyGroup) Wait(ch chan struct{}) {
	n.l.Lock()
	defer n.l.Unlock()
	if n.notify == nil {
		n.notify = make(map[chan struct{}]struct{})
	}
	n.notify[ch] = struct{}{}
}

func (n *NotifyGroup) Clear(ch chan struct{}) {
	n.l.Lock()
	defer n.l.Unlock()
	if n.notify == nil {
		return
	}
	delete(n.notify, ch)
}

func (n *NotifyGroup) WaitCh() chan struct{} {
	ch := make(chan struct{}, 1)
	n.Wait(ch)
	return ch
}
