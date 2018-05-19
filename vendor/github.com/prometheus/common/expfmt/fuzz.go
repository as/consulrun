// Copyright 2014 The Prometheus Authors
//
//

// +build gofuzz

package expfmt

import "bytes"

//
//     go-fuzz-build github.com/prometheus/common/expfmt
//
func Fuzz(in []byte) int {
	parser := TextParser{}
	_, err := parser.TextToMetricFamilies(bytes.NewReader(in))

	if err != nil {
		return 0
	}

	return 1
}
