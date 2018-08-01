// +build !windows

package lock

import (
	"syscall"
)

func signalPid(pid int, sig syscall.Signal) error {
	return syscall.Kill(pid, sig)
}
