package datadog

import (
	"fmt"
	"strings"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/armon/go-metrics"
)

type DogStatsdSink struct {
	client            *statsd.Client
	hostName          string
	propagateHostname bool
}

func NewDogStatsdSink(addr string, hostName string) (*DogStatsdSink, error) {
	client, err := statsd.New(addr)
	if err != nil {
		return nil, err
	}
	sink := &DogStatsdSink{
		client:            client,
		hostName:          hostName,
		propagateHostname: false,
	}
	return sink, nil
}

func (s *DogStatsdSink) SetTags(tags []string) {
	s.client.Tags = tags
}

func (s *DogStatsdSink) EnableHostNamePropagation() {
	s.propagateHostname = true
}

func (s *DogStatsdSink) flattenKey(parts []string) string {
	joined := strings.Join(parts, ".")
	return strings.Map(sanitize, joined)
}

func sanitize(r rune) rune {
	switch r {
	case ':':
		fallthrough
	case ' ':
		return '_'
	default:
		return r
	}
}

func (s *DogStatsdSink) parseKey(key []string) ([]string, []metrics.Label) {

	var labels []metrics.Label
	hostName := s.hostName

	for i, el := range key {
		if el == hostName {
			key = append(key[:i], key[i+1:]...)
			break
		}
	}

	if s.propagateHostname {
		labels = append(labels, metrics.Label{"host", hostName})
	}
	return key, labels
}

func (s *DogStatsdSink) SetGauge(key []string, val float32) {
	s.SetGaugeWithLabels(key, val, nil)
}

func (s *DogStatsdSink) IncrCounter(key []string, val float32) {
	s.IncrCounterWithLabels(key, val, nil)
}

func (s *DogStatsdSink) EmitKey(key []string, val float32) {
}

func (s *DogStatsdSink) AddSample(key []string, val float32) {
	s.AddSampleWithLabels(key, val, nil)
}

func (s *DogStatsdSink) SetGaugeWithLabels(key []string, val float32, labels []metrics.Label) {
	flatKey, tags := s.getFlatkeyAndCombinedLabels(key, labels)
	rate := 1.0
	s.client.Gauge(flatKey, float64(val), tags, rate)
}

func (s *DogStatsdSink) IncrCounterWithLabels(key []string, val float32, labels []metrics.Label) {
	flatKey, tags := s.getFlatkeyAndCombinedLabels(key, labels)
	rate := 1.0
	s.client.Count(flatKey, int64(val), tags, rate)
}

func (s *DogStatsdSink) AddSampleWithLabels(key []string, val float32, labels []metrics.Label) {
	flatKey, tags := s.getFlatkeyAndCombinedLabels(key, labels)
	rate := 1.0
	s.client.TimeInMilliseconds(flatKey, float64(val), tags, rate)
}

func (s *DogStatsdSink) getFlatkeyAndCombinedLabels(key []string, labels []metrics.Label) (string, []string) {
	key, parsedLabels := s.parseKey(key)
	flatKey := s.flattenKey(key)
	labels = append(labels, parsedLabels...)

	var tags []string
	for _, label := range labels {
		label.Name = strings.Map(sanitize, label.Name)
		label.Value = strings.Map(sanitize, label.Value)
		if label.Value != "" {
			tags = append(tags, fmt.Sprintf("%s:%s", label.Name, label.Value))
		} else {
			tags = append(tags, label.Name)
		}
	}

	return flatKey, tags
}