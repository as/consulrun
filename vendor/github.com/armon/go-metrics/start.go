package metrics

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-immutable-radix"
)

type Config struct {
	ServiceName          string        // Prefixed with keys to separate services
	HostName             string        // Hostname to use. If not provided and EnableHostname, it will be os.Hostname
	EnableHostname       bool          // Enable prefixing gauge values with hostname
	EnableHostnameLabel  bool          // Enable adding hostname to labels
	EnableServiceLabel   bool          // Enable adding service to labels
	EnableRuntimeMetrics bool          // Enables profiling of runtime metrics (GC, Goroutines, Memory)
	EnableTypePrefix     bool          // Prefixes key with a type ("counter", "gauge", "timer")
	TimerGranularity     time.Duration // Granularity of timers.
	ProfileInterval      time.Duration // Interval to profile runtime metrics

	AllowedPrefixes []string // A list of metric prefixes to allow, with '.' as the separator
	BlockedPrefixes []string // A list of metric prefixes to block, with '.' as the separator
	FilterDefault   bool     // Whether to allow metrics by default
}

type Metrics struct {
	Config
	lastNumGC  uint32
	sink       MetricSink
	filter     *iradix.Tree
	filterLock sync.RWMutex
}

var globalMetrics atomic.Value // *Metrics

func init() {

	globalMetrics.Store(&Metrics{sink: &BlackholeSink{}})
}

func DefaultConfig(serviceName string) *Config {
	c := &Config{
		ServiceName:          serviceName, // Use client provided service
		HostName:             "",
		EnableHostname:       true,             // Enable hostname prefix
		EnableRuntimeMetrics: true,             // Enable runtime profiling
		EnableTypePrefix:     false,            // Disable type prefix
		TimerGranularity:     time.Millisecond, // Timers are in milliseconds
		ProfileInterval:      time.Second,      // Poll runtime every second
		FilterDefault:        true,             // Don't filter metrics by default
	}

	name, _ := os.Hostname()
	c.HostName = name
	return c
}

func New(conf *Config, sink MetricSink) (*Metrics, error) {
	met := &Metrics{}
	met.Config = *conf
	met.sink = sink
	met.UpdateFilter(conf.AllowedPrefixes, conf.BlockedPrefixes)

	if conf.EnableRuntimeMetrics {
		go met.collectStats()
	}
	return met, nil
}

func NewGlobal(conf *Config, sink MetricSink) (*Metrics, error) {
	metrics, err := New(conf, sink)
	if err == nil {
		globalMetrics.Store(metrics)
	}
	return metrics, err
}

func SetGauge(key []string, val float32) {
	globalMetrics.Load().(*Metrics).SetGauge(key, val)
}

func SetGaugeWithLabels(key []string, val float32, labels []Label) {
	globalMetrics.Load().(*Metrics).SetGaugeWithLabels(key, val, labels)
}

func EmitKey(key []string, val float32) {
	globalMetrics.Load().(*Metrics).EmitKey(key, val)
}

func IncrCounter(key []string, val float32) {
	globalMetrics.Load().(*Metrics).IncrCounter(key, val)
}

func IncrCounterWithLabels(key []string, val float32, labels []Label) {
	globalMetrics.Load().(*Metrics).IncrCounterWithLabels(key, val, labels)
}

func AddSample(key []string, val float32) {
	globalMetrics.Load().(*Metrics).AddSample(key, val)
}

func AddSampleWithLabels(key []string, val float32, labels []Label) {
	globalMetrics.Load().(*Metrics).AddSampleWithLabels(key, val, labels)
}

func MeasureSince(key []string, start time.Time) {
	globalMetrics.Load().(*Metrics).MeasureSince(key, start)
}

func MeasureSinceWithLabels(key []string, start time.Time, labels []Label) {
	globalMetrics.Load().(*Metrics).MeasureSinceWithLabels(key, start, labels)
}

func UpdateFilter(allow, block []string) {
	globalMetrics.Load().(*Metrics).UpdateFilter(allow, block)
}
