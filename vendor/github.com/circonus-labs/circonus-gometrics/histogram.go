// Copyright 2016 Circonus, Inc. All rights reserved.

package circonusgometrics

import (
	"sync"

	"github.com/circonus-labs/circonusllhist"
)

type Histogram struct {
	name string
	hist *circonusllhist.Histogram
	rw   sync.RWMutex
}

func (m *CirconusMetrics) Timing(metric string, val float64) {
	m.SetHistogramValue(metric, val)
}

func (m *CirconusMetrics) RecordValue(metric string, val float64) {
	m.SetHistogramValue(metric, val)
}

func (m *CirconusMetrics) SetHistogramValue(metric string, val float64) {
	hist := m.NewHistogram(metric)

	m.hm.Lock()
	hist.rw.Lock()
	hist.hist.RecordValue(val)
	hist.rw.Unlock()
	m.hm.Unlock()
}

func (m *CirconusMetrics) RemoveHistogram(metric string) {
	m.hm.Lock()
	delete(m.histograms, metric)
	m.hm.Unlock()
}

func (m *CirconusMetrics) NewHistogram(metric string) *Histogram {
	m.hm.Lock()
	defer m.hm.Unlock()

	if hist, ok := m.histograms[metric]; ok {
		return hist
	}

	hist := &Histogram{
		name: metric,
		hist: circonusllhist.New(),
	}

	m.histograms[metric] = hist

	return hist
}

func (h *Histogram) Name() string {
	return h.name
}

func (h *Histogram) RecordValue(v float64) {
	h.rw.Lock()
	h.hist.RecordValue(v)
	h.rw.Unlock()
}
