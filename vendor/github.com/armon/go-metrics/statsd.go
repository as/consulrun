package metrics

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"time"
)

const (
	statsdMaxLen = 1400
)

type StatsdSink struct {
	addr        string
	metricQueue chan string
}

func NewStatsdSinkFromURL(u *url.URL) (MetricSink, error) {
	return NewStatsdSink(u.Host)
}

func NewStatsdSink(addr string) (*StatsdSink, error) {
	s := &StatsdSink{
		addr:        addr,
		metricQueue: make(chan string, 4096),
	}
	go s.flushMetrics()
	return s, nil
}

func (s *StatsdSink) Shutdown() {
	close(s.metricQueue)
}

func (s *StatsdSink) SetGauge(key []string, val float32) {
	flatKey := s.flattenKey(key)
	s.pushMetric(fmt.Sprintf("%s:%f|g\n", flatKey, val))
}

func (s *StatsdSink) SetGaugeWithLabels(key []string, val float32, labels []Label) {
	flatKey := s.flattenKeyLabels(key, labels)
	s.pushMetric(fmt.Sprintf("%s:%f|g\n", flatKey, val))
}

func (s *StatsdSink) EmitKey(key []string, val float32) {
	flatKey := s.flattenKey(key)
	s.pushMetric(fmt.Sprintf("%s:%f|kv\n", flatKey, val))
}

func (s *StatsdSink) IncrCounter(key []string, val float32) {
	flatKey := s.flattenKey(key)
	s.pushMetric(fmt.Sprintf("%s:%f|c\n", flatKey, val))
}

func (s *StatsdSink) IncrCounterWithLabels(key []string, val float32, labels []Label) {
	flatKey := s.flattenKeyLabels(key, labels)
	s.pushMetric(fmt.Sprintf("%s:%f|c\n", flatKey, val))
}

func (s *StatsdSink) AddSample(key []string, val float32) {
	flatKey := s.flattenKey(key)
	s.pushMetric(fmt.Sprintf("%s:%f|ms\n", flatKey, val))
}

func (s *StatsdSink) AddSampleWithLabels(key []string, val float32, labels []Label) {
	flatKey := s.flattenKeyLabels(key, labels)
	s.pushMetric(fmt.Sprintf("%s:%f|ms\n", flatKey, val))
}

func (s *StatsdSink) flattenKey(parts []string) string {
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

func (s *StatsdSink) flattenKeyLabels(parts []string, labels []Label) string {
	for _, label := range labels {
		parts = append(parts, label.Value)
	}
	return s.flattenKey(parts)
}

func (s *StatsdSink) pushMetric(m string) {
	select {
	case s.metricQueue <- m:
	default:
	}
}

func (s *StatsdSink) flushMetrics() {
	var sock net.Conn
	var err error
	var wait <-chan time.Time
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

CONNECT:

	buf := bytes.NewBuffer(nil)

	sock, err = net.Dial("udp", s.addr)
	if err != nil {
		log.Printf("[ERR] Error connecting to statsd! Err: %s", err)
		goto WAIT
	}

	for {
		select {
		case metric, ok := <-s.metricQueue:

			if !ok {
				goto QUIT
			}

			if len(metric)+buf.Len() > statsdMaxLen {
				_, err := sock.Write(buf.Bytes())
				buf.Reset()
				if err != nil {
					log.Printf("[ERR] Error writing to statsd! Err: %s", err)
					goto WAIT
				}
			}

			buf.WriteString(metric)

		case <-ticker.C:
			if buf.Len() == 0 {
				continue
			}

			_, err := sock.Write(buf.Bytes())
			buf.Reset()
			if err != nil {
				log.Printf("[ERR] Error flushing to statsd! Err: %s", err)
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
