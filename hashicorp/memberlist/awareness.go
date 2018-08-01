package memberlist

import (
	"sync"
	"time"

	"github.com/armon/go-metrics"
)

type awareness struct {
	sync.RWMutex

	max int

	score int
}

func newAwareness(max int) *awareness {
	return &awareness{
		max:   max,
		score: 0,
	}
}

func (a *awareness) ApplyDelta(delta int) {
	a.Lock()
	initial := a.score
	a.score += delta
	if a.score < 0 {
		a.score = 0
	} else if a.score > (a.max - 1) {
		a.score = (a.max - 1)
	}
	final := a.score
	a.Unlock()

	if initial != final {
		metrics.SetGauge([]string{"memberlist", "health", "score"}, float32(final))
	}
}

func (a *awareness) GetHealthScore() int {
	a.RLock()
	score := a.score
	a.RUnlock()
	return score
}

func (a *awareness) ScaleTimeout(timeout time.Duration) time.Duration {
	a.RLock()
	score := a.score
	a.RUnlock()
	return timeout * (time.Duration(score) + 1)
}
