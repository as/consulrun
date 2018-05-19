package metrics

import (
	"bytes"
	"fmt"
	"math"
	"net/url"
	"strings"
	"sync"
	"time"
)

type InmemSink struct {
	interval time.Duration

	retain time.Duration

	maxIntervals int

	intervals    []*IntervalMetrics
	intervalLock sync.RWMutex

	rateDenom float64
}

type IntervalMetrics struct {
	sync.RWMutex

	Interval time.Time

	Gauges map[string]GaugeValue

	Points map[string][]float32

	Counters map[string]SampledValue

	Samples map[string]SampledValue
}

func NewIntervalMetrics(intv time.Time) *IntervalMetrics {
	return &IntervalMetrics{
		Interval: intv,
		Gauges:   make(map[string]GaugeValue),
		Points:   make(map[string][]float32),
		Counters: make(map[string]SampledValue),
		Samples:  make(map[string]SampledValue),
	}
}

type AggregateSample struct {
	Count       int       // The count of emitted pairs
	Rate        float64   // The values rate per time unit (usually 1 second)
	Sum         float64   // The sum of values
	SumSq       float64   `json:"-"` // The sum of squared values
	Min         float64   // Minimum value
	Max         float64   // Maximum value
	LastUpdated time.Time `json:"-"` // When value was last updated
}

func (a *AggregateSample) Stddev() float64 {
	num := (float64(a.Count) * a.SumSq) - math.Pow(a.Sum, 2)
	div := float64(a.Count * (a.Count - 1))
	if div == 0 {
		return 0
	}
	return math.Sqrt(num / div)
}

func (a *AggregateSample) Mean() float64 {
	if a.Count == 0 {
		return 0
	}
	return a.Sum / float64(a.Count)
}

func (a *AggregateSample) Ingest(v float64, rateDenom float64) {
	a.Count++
	a.Sum += v
	a.SumSq += (v * v)
	if v < a.Min || a.Count == 1 {
		a.Min = v
	}
	if v > a.Max || a.Count == 1 {
		a.Max = v
	}
	a.Rate = float64(a.Sum) / rateDenom
	a.LastUpdated = time.Now()
}

func (a *AggregateSample) String() string {
	if a.Count == 0 {
		return "Count: 0"
	} else if a.Stddev() == 0 {
		return fmt.Sprintf("Count: %d Sum: %0.3f LastUpdated: %s", a.Count, a.Sum, a.LastUpdated)
	} else {
		return fmt.Sprintf("Count: %d Min: %0.3f Mean: %0.3f Max: %0.3f Stddev: %0.3f Sum: %0.3f LastUpdated: %s",
			a.Count, a.Min, a.Mean(), a.Max, a.Stddev(), a.Sum, a.LastUpdated)
	}
}

func NewInmemSinkFromURL(u *url.URL) (MetricSink, error) {
	params := u.Query()

	interval, err := time.ParseDuration(params.Get("interval"))
	if err != nil {
		return nil, fmt.Errorf("Bad 'interval' param: %s", err)
	}

	retain, err := time.ParseDuration(params.Get("retain"))
	if err != nil {
		return nil, fmt.Errorf("Bad 'retain' param: %s", err)
	}

	return NewInmemSink(interval, retain), nil
}

func NewInmemSink(interval, retain time.Duration) *InmemSink {
	rateTimeUnit := time.Second
	i := &InmemSink{
		interval:     interval,
		retain:       retain,
		maxIntervals: int(retain / interval),
		rateDenom:    float64(interval.Nanoseconds()) / float64(rateTimeUnit.Nanoseconds()),
	}
	i.intervals = make([]*IntervalMetrics, 0, i.maxIntervals)
	return i
}

func (i *InmemSink) SetGauge(key []string, val float32) {
	i.SetGaugeWithLabels(key, val, nil)
}

func (i *InmemSink) SetGaugeWithLabels(key []string, val float32, labels []Label) {
	k, name := i.flattenKeyLabels(key, labels)
	intv := i.getInterval()

	intv.Lock()
	defer intv.Unlock()
	intv.Gauges[k] = GaugeValue{Name: name, Value: val, Labels: labels}
}

func (i *InmemSink) EmitKey(key []string, val float32) {
	k := i.flattenKey(key)
	intv := i.getInterval()

	intv.Lock()
	defer intv.Unlock()
	vals := intv.Points[k]
	intv.Points[k] = append(vals, val)
}

func (i *InmemSink) IncrCounter(key []string, val float32) {
	i.IncrCounterWithLabels(key, val, nil)
}

func (i *InmemSink) IncrCounterWithLabels(key []string, val float32, labels []Label) {
	k, name := i.flattenKeyLabels(key, labels)
	intv := i.getInterval()

	intv.Lock()
	defer intv.Unlock()

	agg, ok := intv.Counters[k]
	if !ok {
		agg = SampledValue{
			Name:            name,
			AggregateSample: &AggregateSample{},
			Labels:          labels,
		}
		intv.Counters[k] = agg
	}
	agg.Ingest(float64(val), i.rateDenom)
}

func (i *InmemSink) AddSample(key []string, val float32) {
	i.AddSampleWithLabels(key, val, nil)
}

func (i *InmemSink) AddSampleWithLabels(key []string, val float32, labels []Label) {
	k, name := i.flattenKeyLabels(key, labels)
	intv := i.getInterval()

	intv.Lock()
	defer intv.Unlock()

	agg, ok := intv.Samples[k]
	if !ok {
		agg = SampledValue{
			Name:            name,
			AggregateSample: &AggregateSample{},
			Labels:          labels,
		}
		intv.Samples[k] = agg
	}
	agg.Ingest(float64(val), i.rateDenom)
}

func (i *InmemSink) Data() []*IntervalMetrics {

	i.getInterval()

	i.intervalLock.RLock()
	defer i.intervalLock.RUnlock()

	n := len(i.intervals)
	intervals := make([]*IntervalMetrics, n)

	copy(intervals[:n-1], i.intervals[:n-1])
	current := i.intervals[n-1]

	intervals[n-1] = &IntervalMetrics{}
	copyCurrent := intervals[n-1]
	current.RLock()
	*copyCurrent = *current

	copyCurrent.Gauges = make(map[string]GaugeValue, len(current.Gauges))
	for k, v := range current.Gauges {
		copyCurrent.Gauges[k] = v
	}

	copyCurrent.Points = make(map[string][]float32, len(current.Points))
	for k, v := range current.Points {
		copyCurrent.Points[k] = v
	}
	copyCurrent.Counters = make(map[string]SampledValue, len(current.Counters))
	for k, v := range current.Counters {
		copyCurrent.Counters[k] = v
	}
	copyCurrent.Samples = make(map[string]SampledValue, len(current.Samples))
	for k, v := range current.Samples {
		copyCurrent.Samples[k] = v
	}
	current.RUnlock()

	return intervals
}

func (i *InmemSink) getExistingInterval(intv time.Time) *IntervalMetrics {
	i.intervalLock.RLock()
	defer i.intervalLock.RUnlock()

	n := len(i.intervals)
	if n > 0 && i.intervals[n-1].Interval == intv {
		return i.intervals[n-1]
	}
	return nil
}

func (i *InmemSink) createInterval(intv time.Time) *IntervalMetrics {
	i.intervalLock.Lock()
	defer i.intervalLock.Unlock()

	n := len(i.intervals)
	if n > 0 && i.intervals[n-1].Interval == intv {
		return i.intervals[n-1]
	}

	current := NewIntervalMetrics(intv)
	i.intervals = append(i.intervals, current)
	n++

	if n >= i.maxIntervals {
		copy(i.intervals[0:], i.intervals[n-i.maxIntervals:])
		i.intervals = i.intervals[:i.maxIntervals]
	}
	return current
}

func (i *InmemSink) getInterval() *IntervalMetrics {
	intv := time.Now().Truncate(i.interval)
	if m := i.getExistingInterval(intv); m != nil {
		return m
	}
	return i.createInterval(intv)
}

func (i *InmemSink) flattenKey(parts []string) string {
	buf := &bytes.Buffer{}
	replacer := strings.NewReplacer(" ", "_")

	if len(parts) > 0 {
		replacer.WriteString(buf, parts[0])
	}
	for _, part := range parts[1:] {
		replacer.WriteString(buf, ".")
		replacer.WriteString(buf, part)
	}

	return buf.String()
}

func (i *InmemSink) flattenKeyLabels(parts []string, labels []Label) (string, string) {
	buf := &bytes.Buffer{}
	replacer := strings.NewReplacer(" ", "_")

	if len(parts) > 0 {
		replacer.WriteString(buf, parts[0])
	}
	for _, part := range parts[1:] {
		replacer.WriteString(buf, ".")
		replacer.WriteString(buf, part)
	}

	key := buf.String()

	for _, label := range labels {
		replacer.WriteString(buf, fmt.Sprintf(";%s=%s", label.Name, label.Value))
	}

	return buf.String(), key
}
