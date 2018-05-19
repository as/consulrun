// +build windows

package metrics

import (
	"syscall"
)

const (
	DefaultSignal = syscall.Signal(21)
)
