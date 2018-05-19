// Copyright 2016 The Go Authors. All rights reserved.

// +build go1.6

package http2

import (
	"net/http"
	"time"
)

func transportExpectContinueTimeout(t1 *http.Transport) time.Duration {
	return t1.ExpectContinueTimeout
}
