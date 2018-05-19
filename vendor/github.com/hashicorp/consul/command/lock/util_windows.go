// +build windows

package lock

import (
	"os"
	"syscall"
)

func signalPid(pid int, sig syscall.Signal) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	_ = sig
	return p.Signal(syscall.SIGKILL)
}
