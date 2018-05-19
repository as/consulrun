package metrics

import (
	"runtime"
	"strings"
	"time"

	"github.com/hashicorp/go-immutable-radix"
)

type Label struct {
	Name  string
	Value string
}

func (m *Metrics) SetGauge(key []string, val float32) {
	m.SetGaugeWithLabels(key, val, nil)
}

func (m *Metrics) SetGaugeWithLabels(key []string, val float32, labels []Label) {
	if m.HostName != "" {
		if m.EnableHostnameLabel {
			labels = append(labels, Label{"host", m.HostName})
		} else if m.EnableHostname {
			key = insert(0, m.HostName, key)
		}
	}
	if m.EnableTypePrefix {
		key = insert(0, "gauge", key)
	}
	if m.ServiceName != "" {
		if m.EnableServiceLabel {
			labels = append(labels, Label{"service", m.ServiceName})
		} else {
			key = insert(0, m.ServiceName, key)
		}
	}
	if !m.allowMetric(key) {
		return
	}
	m.sink.SetGaugeWithLabels(key, val, labels)
}

func (m *Metrics) EmitKey(key []string, val float32) {
	if m.EnableTypePrefix {
		key = insert(0, "kv", key)
	}
	if m.ServiceName != "" {
		key = insert(0, m.ServiceName, key)
	}
	if !m.allowMetric(key) {
		return
	}
	m.sink.EmitKey(key, val)
}

func (m *Metrics) IncrCounter(key []string, val float32) {
	m.IncrCounterWithLabels(key, val, nil)
}

func (m *Metrics) IncrCounterWithLabels(key []string, val float32, labels []Label) {
	if m.HostName != "" && m.EnableHostnameLabel {
		labels = append(labels, Label{"host", m.HostName})
	}
	if m.EnableTypePrefix {
		key = insert(0, "counter", key)
	}
	if m.ServiceName != "" {
		if m.EnableServiceLabel {
			labels = append(labels, Label{"service", m.ServiceName})
		} else {
			key = insert(0, m.ServiceName, key)
		}
	}
	if !m.allowMetric(key) {
		return
	}
	m.sink.IncrCounterWithLabels(key, val, labels)
}

func (m *Metrics) AddSample(key []string, val float32) {
	m.AddSampleWithLabels(key, val, nil)
}

func (m *Metrics) AddSampleWithLabels(key []string, val float32, labels []Label) {
	if m.HostName != "" && m.EnableHostnameLabel {
		labels = append(labels, Label{"host", m.HostName})
	}
	if m.EnableTypePrefix {
		key = insert(0, "sample", key)
	}
	if m.ServiceName != "" {
		if m.EnableServiceLabel {
			labels = append(labels, Label{"service", m.ServiceName})
		} else {
			key = insert(0, m.ServiceName, key)
		}
	}
	if !m.allowMetric(key) {
		return
	}
	m.sink.AddSampleWithLabels(key, val, labels)
}

func (m *Metrics) MeasureSince(key []string, start time.Time) {
	m.MeasureSinceWithLabels(key, start, nil)
}

func (m *Metrics) MeasureSinceWithLabels(key []string, start time.Time, labels []Label) {
	if m.HostName != "" && m.EnableHostnameLabel {
		labels = append(labels, Label{"host", m.HostName})
	}
	if m.EnableTypePrefix {
		key = insert(0, "timer", key)
	}
	if m.ServiceName != "" {
		if m.EnableServiceLabel {
			labels = append(labels, Label{"service", m.ServiceName})
		} else {
			key = insert(0, m.ServiceName, key)
		}
	}
	if !m.allowMetric(key) {
		return
	}
	now := time.Now()
	elapsed := now.Sub(start)
	msec := float32(elapsed.Nanoseconds()) / float32(m.TimerGranularity)
	m.sink.AddSampleWithLabels(key, msec, labels)
}

func (m *Metrics) UpdateFilter(allow, block []string) {
	m.filterLock.Lock()
	defer m.filterLock.Unlock()

	m.AllowedPrefixes = allow
	m.BlockedPrefixes = block

	m.filter = iradix.New()
	for _, prefix := range m.AllowedPrefixes {
		m.filter, _, _ = m.filter.Insert([]byte(prefix), true)
	}
	for _, prefix := range m.BlockedPrefixes {
		m.filter, _, _ = m.filter.Insert([]byte(prefix), false)
	}
}

func (m *Metrics) allowMetric(key []string) bool {
	m.filterLock.RLock()
	defer m.filterLock.RUnlock()

	if m.filter == nil || m.filter.Len() == 0 {
		return m.Config.FilterDefault
	}

	_, allowed, ok := m.filter.Root().LongestPrefix([]byte(strings.Join(key, ".")))
	if !ok {
		return m.Config.FilterDefault
	}
	return allowed.(bool)
}

func (m *Metrics) collectStats() {
	for {
		time.Sleep(m.ProfileInterval)
		m.emitRuntimeStats()
	}
}

func (m *Metrics) emitRuntimeStats() {

	numRoutines := runtime.NumGoroutine()
	m.SetGauge([]string{"runtime", "num_goroutines"}, float32(numRoutines))

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	m.SetGauge([]string{"runtime", "alloc_bytes"}, float32(stats.Alloc))
	m.SetGauge([]string{"runtime", "sys_bytes"}, float32(stats.Sys))
	m.SetGauge([]string{"runtime", "malloc_count"}, float32(stats.Mallocs))
	m.SetGauge([]string{"runtime", "free_count"}, float32(stats.Frees))
	m.SetGauge([]string{"runtime", "heap_objects"}, float32(stats.HeapObjects))
	m.SetGauge([]string{"runtime", "total_gc_pause_ns"}, float32(stats.PauseTotalNs))
	m.SetGauge([]string{"runtime", "total_gc_runs"}, float32(stats.NumGC))

	num := stats.NumGC

	if num < m.lastNumGC {
		m.lastNumGC = 0
	}

	if num-m.lastNumGC >= 256 {
		m.lastNumGC = num - 255
	}

	for i := m.lastNumGC; i < num; i++ {
		pause := stats.PauseNs[i%256]
		m.AddSample([]string{"runtime", "gc_pause_ns"}, float32(pause))
	}
	m.lastNumGC = num
}

func insert(i int, v string, s []string) []string {
	s = append(s, "")
	copy(s[i+1:], s[i:])
	s[i] = v
	return s
}
