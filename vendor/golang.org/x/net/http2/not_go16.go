// Copyright 2015 The Go Authors. All rights reserved.

// +build !go1.6

package http2

import (
	"net/http"
	"time"
)

func configureTransport(t1 *http.Transport) (*Transport, error) {
	return nil, errTransportVersion
}

func transportExpectContinueTimeout(t1 *http.Transport) time.Duration {
	return 0

}
