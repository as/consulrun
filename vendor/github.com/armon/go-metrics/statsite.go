package metrics

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"time"
)

const (
	flushInterval = 100 * time.Millisecond
)

func NewStatsiteSinkFromURL(u *url.URL) (MetricSink, error) {
	return NewStatsiteSink(u.Host)
}

type StatsiteSink struct {
	addr        string
	metricQueue chan string
}

func NewStatsiteSink(addr string) (*StatsiteSink, error) {
	s := &StatsiteSink{
		addr:        addr,
		metricQueue: make(chan string, 4096),
	}
	go s.flushMetrics()
	return s, nil
}

func (s *StatsiteSink) Shutdown() {
	close(s.metricQueue)
}

func (s *StatsiteSink) SetGauge(key []string, val float32) {
	flatKey := s.flattenKey(key)
	s.pushMetric(fmt.Sprintf("%s:%f|g\n", flatKey, val))
}

func (s *StatsiteSink) SetGaugeWithLabels(key []string, val float32, labels []Label) {
	flatKey := s.flattenKeyLabels(key, labels)
	s.pushMetric(fmt.Sprintf("%s:%f|g\n", flatKey, val))
}

func (s *StatsiteSink) EmitKey(key []string, val float32) {
	flatKey := s.flattenKey(key)
	s.pushMetric(fmt.Sprintf("%s:%f|kv\n", flatKey, val))
}

func (s *StatsiteSink) IncrCounter(key []string, val float32) {
	flatKey := s.flattenKey(key)
	s.pushMetric(fmt.Sprintf("%s:%f|c\n", flatKey, val))
}

func (s *StatsiteSink) IncrCounterWithLabels(key []string, val float32, labels []Label) {
	flatKey := s.flattenKeyLabels(key, labels)
	s.pushMetric(fmt.Sprintf("%s:%f|c\n", flatKey, val))
}

func (s *StatsiteSink) AddSample(key []string, val float32) {
	flatKey := s.flattenKey(key)
	s.pushMetric(fmt.Sprintf("%s:%f|ms\n", flatKey, val))
}

func (s *StatsiteSink) AddSampleWithLabels(key []string, val float32, labels []Label) {
	flatKey := s.flattenKeyLabels(key, labels)
	s.pushMetric(fmt.Sprintf("%s:%f|ms\n", flatKey, val))
}

func (s *StatsiteSink) flattenKey(parts []string) string {
	joined := strings.Join(parts, ".")
	return strings.Map(func(r rune) rune {
		switch r {
		case ':':
			fallthrough
		case ' ':
			return '_'
		default:
			return r
		}
	}, joined)
}

func (s *StatsiteSink) flattenKeyLabels(parts []string, labels []Label) string {
	for _, label := range labels {
		parts = append(parts, label.Value)
	}
	return s.flattenKey(parts)
}

func (s *StatsiteSink) pushMetric(m string) {
	select {
	case s.metricQueue <- m:
	default:
	}
}

func (s *StatsiteSink) flushMetrics() {
	var sock net.Conn
	var err error
	var wait <-chan time.Time
	var buffered *bufio.Writer
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

CONNECT:

	sock, err = net.Dial("tcp", s.addr)
	if err != nil {
		log.Printf("[ERR] Error connecting to statsite! Err: %s", err)
		goto WAIT
	}

	buffered = bufio.NewWriter(sock)

	for {
		select {
		case metric, ok := <-s.metricQueue:

			if !ok {
				goto QUIT
			}

			_, err := buffered.Write([]byte(metric))
			if err != nil {
				log.Printf("[ERR] Error writing to statsite! Err: %s", err)
				goto WAIT
			}
		case <-ticker.C:
			if err := buffered.Flush(); err != nil {
				log.Printf("[ERR] Error flushing to statsite! Err: %s", err)
				goto WAIT
			}
		}
	}

WAIT:

	wait = time.After(time.Duration(5) * time.Second)
	for {
		select {

		case _, ok := <-s.metricQueue:
			if !ok {
				goto QUIT
			}
		case <-wait:
			goto CONNECT
		}
	}
QUIT:
	s.metricQueue = nil
}
