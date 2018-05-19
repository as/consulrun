// Copyright 2016 Circonus, Inc. All rights reserved.

//
//
//
//
//
//
//
//
package circonusgometrics

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/circonus-labs/circonus-gometrics/api"
	"github.com/circonus-labs/circonus-gometrics/checkmgr"
)

const (
	defaultFlushInterval = "10s" // 10 * time.Second
)

type Config struct {
	Log             *log.Logger
	Debug           bool
	ResetCounters   string // reset/delete counters on flush (default true)
	ResetGauges     string // reset/delete gauges on flush (default true)
	ResetHistograms string // reset/delete histograms on flush (default true)
	ResetText       string // reset/delete text on flush (default true)

	CheckManager checkmgr.Config

	Interval string
}

type CirconusMetrics struct {
	Log             *log.Logger
	Debug           bool
	resetCounters   bool
	resetGauges     bool
	resetHistograms bool
	resetText       bool
	flushInterval   time.Duration
	flushing        bool
	flushmu         sync.Mutex
	check           *checkmgr.CheckManager

	counters map[string]uint64
	cm       sync.Mutex

	counterFuncs map[string]func() uint64
	cfm          sync.Mutex

	gauges map[string]string
	gm     sync.Mutex

	gaugeFuncs map[string]func() int64
	gfm        sync.Mutex

	histograms map[string]*Histogram
	hm         sync.Mutex

	text map[string]string
	tm   sync.Mutex

	textFuncs map[string]func() string
	tfm       sync.Mutex
}

func NewCirconusMetrics(cfg *Config) (*CirconusMetrics, error) {

	if cfg == nil {
		return nil, errors.New("Invalid configuration (nil).")
	}

	cm := &CirconusMetrics{
		counters:     make(map[string]uint64),
		counterFuncs: make(map[string]func() uint64),
		gauges:       make(map[string]string),
		gaugeFuncs:   make(map[string]func() int64),
		histograms:   make(map[string]*Histogram),
		text:         make(map[string]string),
		textFuncs:    make(map[string]func() string),
	}

	cm.Debug = cfg.Debug
	cm.Log = cfg.Log

	if cm.Debug && cfg.Log == nil {
		cm.Log = log.New(os.Stderr, "", log.LstdFlags)
	}
	if cm.Log == nil {
		cm.Log = log.New(ioutil.Discard, "", log.LstdFlags)
	}

	fi := defaultFlushInterval
	if cfg.Interval != "" {
		fi = cfg.Interval
	}

	dur, err := time.ParseDuration(fi)
	if err != nil {
		return nil, err
	}
	cm.flushInterval = dur

	var setting bool

	cm.resetCounters = true
	if cfg.ResetCounters != "" {
		if setting, err = strconv.ParseBool(cfg.ResetCounters); err == nil {
			cm.resetCounters = setting
		}
	}

	cm.resetGauges = true
	if cfg.ResetGauges != "" {
		if setting, err = strconv.ParseBool(cfg.ResetGauges); err == nil {
			cm.resetGauges = setting
		}
	}

	cm.resetHistograms = true
	if cfg.ResetHistograms != "" {
		if setting, err = strconv.ParseBool(cfg.ResetHistograms); err == nil {
			cm.resetHistograms = setting
		}
	}

	cm.resetText = true
	if cfg.ResetText != "" {
		if setting, err = strconv.ParseBool(cfg.ResetText); err == nil {
			cm.resetText = setting
		}
	}

	cfg.CheckManager.Debug = cm.Debug
	cfg.CheckManager.Log = cm.Log

	check, err := checkmgr.NewCheckManager(&cfg.CheckManager)
	if err != nil {
		return nil, err
	}
	cm.check = check

	if _, err := cm.check.GetTrap(); err != nil {
		return nil, err
	}

	return cm, nil
}

func (m *CirconusMetrics) Start() {
	go func() {
		for range time.NewTicker(m.flushInterval).C {
			m.Flush()
		}
	}()
}

func (m *CirconusMetrics) Flush() {
	if m.flushing {
		return
	}
	m.flushmu.Lock()
	m.flushing = true
	m.flushmu.Unlock()

	if m.Debug {
		m.Log.Println("[DEBUG] Flushing metrics")
	}

	newMetrics := make(map[string]*api.CheckBundleMetric)

	counters, gauges, histograms, text := m.snapshot()
	output := make(map[string]interface{})
	for name, value := range counters {
		send := m.check.IsMetricActive(name)
		if !send && m.check.ActivateMetric(name) {
			send = true
			newMetrics[name] = &api.CheckBundleMetric{
				Name:   name,
				Type:   "numeric",
				Status: "active",
			}
		}
		if send {
			output[name] = map[string]interface{}{
				"_type":  "n",
				"_value": value,
			}
		}
	}

	for name, value := range gauges {
		send := m.check.IsMetricActive(name)
		if !send && m.check.ActivateMetric(name) {
			send = true
			newMetrics[name] = &api.CheckBundleMetric{
				Name:   name,
				Type:   "numeric",
				Status: "active",
			}
		}
		if send {
			output[name] = map[string]interface{}{
				"_type":  "n",
				"_value": value,
			}
		}
	}

	for name, value := range histograms {
		send := m.check.IsMetricActive(name)
		if !send && m.check.ActivateMetric(name) {
			send = true
			newMetrics[name] = &api.CheckBundleMetric{
				Name:   name,
				Type:   "histogram",
				Status: "active",
			}
		}
		if send {
			output[name] = map[string]interface{}{
				"_type":  "n",
				"_value": value.DecStrings(),
			}
		}
	}

	for name, value := range text {
		send := m.check.IsMetricActive(name)
		if !send && m.check.ActivateMetric(name) {
			send = true
			newMetrics[name] = &api.CheckBundleMetric{
				Name:   name,
				Type:   "text",
				Status: "active",
			}
		}
		if send {
			output[name] = map[string]interface{}{
				"_type":  "s",
				"_value": value,
			}
		}
	}

	if len(output) > 0 {
		m.submit(output, newMetrics)
	} else {
		if m.Debug {
			m.Log.Println("[DEBUG] No metrics to send, skipping")
		}
	}

	m.flushmu.Lock()
	m.flushing = false
	m.flushmu.Unlock()
}
