// Copyright 2016 Circonus, Inc. All rights reserved.

package circonusgometrics

import (
	"net/http"
	"time"
)

func (m *CirconusMetrics) TrackHTTPLatency(name string, handler func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		start := time.Now().UnixNano()
		handler(rw, req)
		elapsed := time.Now().UnixNano() - start

		m.RecordValue("go`HTTP`"+req.Method+"`"+name+"`latency", float64(elapsed)/float64(time.Second))
	}
}
