// +build go1.3
package prometheus

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"regexp"

	"github.com/armon/go-metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	DefaultPrometheusOpts = PrometheusOpts{
		Expiration: 60 * time.Second,
	}
)

type PrometheusOpts struct {
	Expiration time.Duration
}

type PrometheusSink struct {
	mu         sync.Mutex
	gauges     map[string]prometheus.Gauge
	summaries  map[string]prometheus.Summary
	counters   map[string]prometheus.Counter
	updates    map[string]time.Time
	expiration time.Duration
}

func NewPrometheusSink() (*PrometheusSink, error) {
	return NewPrometheusSinkFrom(DefaultPrometheusOpts)
}

func NewPrometheusSinkFrom(opts PrometheusOpts) (*PrometheusSink, error) {
	sink := &PrometheusSink{
		gauges:     make(map[string]prometheus.Gauge),
		summaries:  make(map[string]prometheus.Summary),
		counters:   make(map[string]prometheus.Counter),
		updates:    make(map[string]time.Time),
		expiration: opts.Expiration,
	}

	return sink, prometheus.Register(sink)
}

func (p *PrometheusSink) Describe(c chan<- *prometheus.Desc) {

	prometheus.NewGauge(prometheus.GaugeOpts{Name: "Dummy", Help: "Dummy"}).Describe(c)
}

func (p *PrometheusSink) Collect(c chan<- prometheus.Metric) {
	p.mu.Lock()
	defer p.mu.Unlock()

	expire := p.expiration != 0
	now := time.Now()
	for k, v := range p.gauges {
		last := p.updates[k]
		if expire && last.Add(p.expiration).Before(now) {
			delete(p.updates, k)
			delete(p.gauges, k)
		} else {
			v.Collect(c)
		}
	}
	for k, v := range p.summaries {
		last := p.updates[k]
		if expire && last.Add(p.expiration).Before(now) {
			delete(p.updates, k)
			delete(p.summaries, k)
		} else {
			v.Collect(c)
		}
	}
	for k, v := range p.counters {
		last := p.updates[k]
		if expire && last.Add(p.expiration).Before(now) {
			delete(p.updates, k)
			delete(p.counters, k)
		} else {
			v.Collect(c)
		}
	}
}

var forbiddenChars = regexp.MustCompile("[ .=\\-]")

func (p *PrometheusSink) flattenKey(parts []string, labels []metrics.Label) (string, string) {
	key := strings.Join(parts, "_")
	key = forbiddenChars.ReplaceAllString(key, "_")

	hash := key
	for _, label := range labels {
		hash += fmt.Sprintf(";%s=%s", label.Name, label.Value)
	}

	return key, hash
}

func prometheusLabels(labels []metrics.Label) prometheus.Labels {
	l := make(prometheus.Labels)
	for _, label := range labels {
		l[label.Name] = label.Value
	}
	return l
}

func (p *PrometheusSink) SetGauge(parts []string, val float32) {
	p.SetGaugeWithLabels(parts, val, nil)
}

func (p *PrometheusSink) SetGaugeWithLabels(parts []string, val float32, labels []metrics.Label) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key, hash := p.flattenKey(parts, labels)
	g, ok := p.gauges[hash]
	if !ok {
		g = prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        key,
			Help:        key,
			ConstLabels: prometheusLabels(labels),
		})
		p.gauges[hash] = g
	}
	g.Set(float64(val))
	p.updates[hash] = time.Now()
}

func (p *PrometheusSink) AddSample(parts []string, val float32) {
	p.AddSampleWithLabels(parts, val, nil)
}

func (p *PrometheusSink) AddSampleWithLabels(parts []string, val float32, labels []metrics.Label) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key, hash := p.flattenKey(parts, labels)
	g, ok := p.summaries[hash]
	if !ok {
		g = prometheus.NewSummary(prometheus.SummaryOpts{
			Name:        key,
			Help:        key,
			MaxAge:      10 * time.Second,
			ConstLabels: prometheusLabels(labels),
		})
		p.summaries[hash] = g
	}
	g.Observe(float64(val))
	p.updates[hash] = time.Now()
}

func (p *PrometheusSink) EmitKey(key []string, val float32) {
}

func (p *PrometheusSink) IncrCounter(parts []string, val float32) {
	p.IncrCounterWithLabels(parts, val, nil)
}

func (p *PrometheusSink) IncrCounterWithLabels(parts []string, val float32, labels []metrics.Label) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key, hash := p.flattenKey(parts, labels)
	g, ok := p.counters[hash]
	if !ok {
		g = prometheus.NewCounter(prometheus.CounterOpts{
			Name:        key,
			Help:        key,
			ConstLabels: prometheusLabels(labels),
		})
		p.counters[hash] = g
	}
	g.Add(float64(val))
	p.updates[hash] = time.Now()
}
