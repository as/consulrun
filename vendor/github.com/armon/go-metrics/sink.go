package metrics

import (
	"fmt"
	"net/url"
)

type MetricSink interface {
	SetGauge(key []string, val float32)
	SetGaugeWithLabels(key []string, val float32, labels []Label)

	EmitKey(key []string, val float32)

	IncrCounter(key []string, val float32)
	IncrCounterWithLabels(key []string, val float32, labels []Label)

	AddSample(key []string, val float32)
	AddSampleWithLabels(key []string, val float32, labels []Label)
}

type BlackholeSink struct{}

func (*BlackholeSink) SetGauge(key []string, val float32)                              {}
func (*BlackholeSink) SetGaugeWithLabels(key []string, val float32, labels []Label)    {}
func (*BlackholeSink) EmitKey(key []string, val float32)                               {}
func (*BlackholeSink) IncrCounter(key []string, val float32)                           {}
func (*BlackholeSink) IncrCounterWithLabels(key []string, val float32, labels []Label) {}
func (*BlackholeSink) AddSample(key []string, val float32)                             {}
func (*BlackholeSink) AddSampleWithLabels(key []string, val float32, labels []Label)   {}

type FanoutSink []MetricSink

func (fh FanoutSink) SetGauge(key []string, val float32) {
	fh.SetGaugeWithLabels(key, val, nil)
}

func (fh FanoutSink) SetGaugeWithLabels(key []string, val float32, labels []Label) {
	for _, s := range fh {
		s.SetGaugeWithLabels(key, val, labels)
	}
}

func (fh FanoutSink) EmitKey(key []string, val float32) {
	for _, s := range fh {
		s.EmitKey(key, val)
	}
}

func (fh FanoutSink) IncrCounter(key []string, val float32) {
	fh.IncrCounterWithLabels(key, val, nil)
}

func (fh FanoutSink) IncrCounterWithLabels(key []string, val float32, labels []Label) {
	for _, s := range fh {
		s.IncrCounterWithLabels(key, val, labels)
	}
}

func (fh FanoutSink) AddSample(key []string, val float32) {
	fh.AddSampleWithLabels(key, val, nil)
}

func (fh FanoutSink) AddSampleWithLabels(key []string, val float32, labels []Label) {
	for _, s := range fh {
		s.AddSampleWithLabels(key, val, labels)
	}
}

type sinkURLFactoryFunc func(*url.URL) (MetricSink, error)

var sinkRegistry = map[string]sinkURLFactoryFunc{
	"statsd":   NewStatsdSinkFromURL,
	"statsite": NewStatsiteSinkFromURL,
	"inmem":    NewInmemSinkFromURL,
}

//
//
//
func NewMetricSinkFromURL(urlStr string) (MetricSink, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	sinkURLFactoryFunc := sinkRegistry[u.Scheme]
	if sinkURLFactoryFunc == nil {
		return nil, fmt.Errorf(
			"cannot create metric sink, unrecognized sink name: %q", u.Scheme)
	}

	return sinkURLFactoryFunc(u)
}
