// Circonus Metrics Sink

package circonus

import (
	"strings"

	"github.com/armon/go-metrics"
	cgm "github.com/circonus-labs/circonus-gometrics"
)

type CirconusSink struct {
	metrics *cgm.CirconusMetrics
}

type Config cgm.Config

//
//
func NewCirconusSink(cc *Config) (*CirconusSink, error) {
	cfg := cgm.Config{}
	if cc != nil {
		cfg = cgm.Config(*cc)
	}

	metrics, err := cgm.NewCirconusMetrics(&cfg)
	if err != nil {
		return nil, err
	}

	return &CirconusSink{
		metrics: metrics,
	}, nil
}

func (s *CirconusSink) Start() {
	s.metrics.Start()
}

func (s *CirconusSink) Flush() {
	s.metrics.Flush()
}

func (s *CirconusSink) SetGauge(key []string, val float32) {
	flatKey := s.flattenKey(key)
	s.metrics.SetGauge(flatKey, int64(val))
}

func (s *CirconusSink) SetGaugeWithLabels(key []string, val float32, labels []metrics.Label) {
	flatKey := s.flattenKeyLabels(key, labels)
	s.metrics.SetGauge(flatKey, int64(val))
}

func (s *CirconusSink) EmitKey(key []string, val float32) {

}

func (s *CirconusSink) IncrCounter(key []string, val float32) {
	flatKey := s.flattenKey(key)
	s.metrics.IncrementByValue(flatKey, uint64(val))
}

func (s *CirconusSink) IncrCounterWithLabels(key []string, val float32, labels []metrics.Label) {
	flatKey := s.flattenKeyLabels(key, labels)
	s.metrics.IncrementByValue(flatKey, uint64(val))
}

func (s *CirconusSink) AddSample(key []string, val float32) {
	flatKey := s.flattenKey(key)
	s.metrics.RecordValue(flatKey, float64(val))
}

func (s *CirconusSink) AddSampleWithLabels(key []string, val float32, labels []metrics.Label) {
	flatKey := s.flattenKeyLabels(key, labels)
	s.metrics.RecordValue(flatKey, float64(val))
}

func (s *CirconusSink) flattenKey(parts []string) string {
	joined := strings.Join(parts, "`")
	return strings.Map(func(r rune) rune {
		switch r {
		case ' ':
			return '_'
		default:
			return r
		}
	}, joined)
}

func (s *CirconusSink) flattenKeyLabels(parts []string, labels []metrics.Label) string {
	for _, label := range labels {
		parts = append(parts, label.Value)
	}
	return s.flattenKey(parts)
}
