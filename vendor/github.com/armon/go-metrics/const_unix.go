// +build !windows

package metrics

import (
	"syscall"
)

const (
	DefaultSignal = syscall.SIGUSR1
)
