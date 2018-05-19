// Copyright 2016 The Go Authors. All rights reserved.

// +build !go1.9

package http2

import (
	"net/http"
)

func configureServer19(s *http.Server, conf *Server) error {

	return nil
}
