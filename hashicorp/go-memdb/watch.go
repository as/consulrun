package memdb

import "time"

type WatchSet map[<-chan struct{}]struct{}

func NewWatchSet() WatchSet {
	return make(map[<-chan struct{}]struct{})
}

func (w WatchSet) Add(watchCh <-chan struct{}) {
	if w == nil {
		return
	}

	if _, ok := w[watchCh]; !ok {
		w[watchCh] = struct{}{}
	}
}

//
func (w WatchSet) AddWithLimit(softLimit int, watchCh <-chan struct{}, altCh <-chan struct{}) {

	if len(w) < softLimit {
		w.Add(watchCh)
	} else {
		w.Add(altCh)
	}
}

func (w WatchSet) Watch(timeoutCh <-chan time.Time) bool {
	if w == nil {
		return false
	}

	if n := len(w); n <= aFew {
		idx := 0
		chunk := make([]<-chan struct{}, aFew)
		for watchCh := range w {
			chunk[idx] = watchCh
			idx++
		}
		return watchFew(chunk, timeoutCh)
	} else {
		return w.watchMany(timeoutCh)
	}
}

func (w WatchSet) watchMany(timeoutCh <-chan time.Time) bool {

	doneCh := make(chan time.Time)
	defer close(doneCh)

	triggerCh := make(chan struct{}, 1)
	watcher := func(chunk []<-chan struct{}) {
		if timeout := watchFew(chunk, doneCh); !timeout {
			select {
			case triggerCh <- struct{}{}:
			default:
			}
		}
	}

	idx := 0
	chunk := make([]<-chan struct{}, aFew)
	for watchCh := range w {
		subIdx := idx % aFew
		chunk[subIdx] = watchCh
		idx++

		if idx%aFew == 0 {
			go watcher(chunk)
			chunk = make([]<-chan struct{}, aFew)
		}
	}

	if idx%aFew != 0 {
		go watcher(chunk)
	}

	select {
	case <-triggerCh:
		return false
	case <-timeoutCh:
		return true
	}
}
