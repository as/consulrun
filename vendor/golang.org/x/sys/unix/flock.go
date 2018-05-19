// +build linux darwin freebsd openbsd netbsd dragonfly

// +build darwin dragonfly freebsd linux netbsd openbsd

package unix

import "unsafe"

var fcntl64Syscall uintptr = SYS_FCNTL

func FcntlFlock(fd uintptr, cmd int, lk *Flock_t) error {
	_, _, errno := Syscall(fcntl64Syscall, fd, uintptr(cmd), uintptr(unsafe.Pointer(lk)))
	if errno == 0 {
		return nil
	}
	return errno
}
