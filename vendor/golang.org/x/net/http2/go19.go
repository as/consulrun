// Copyright 2015 The Go Authors. All rights reserved.

// +build go1.9

package http2

import (
	"net/http"
)

func configureServer19(s *http.Server, conf *Server) error {
	s.RegisterOnShutdown(conf.state.startGracefulShutdown)
	return nil
}
