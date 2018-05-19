package bolt

import (
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"
)

// LockFileEx code derived from golang build filemutex_windows.go @ v1.5.1
var (
	modkernel32      = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = modkernel32.NewProc("LockFileEx")
	procUnlockFileEx = modkernel32.NewProc("UnlockFileEx")
)

const (
	lockExt = ".lock"

	flagLockExclusive       = 2
	flagLockFailImmediately = 1

	errLockViolation syscall.Errno = 0x21
)

func lockFileEx(h syscall.Handle, flags, reserved, locklow, lockhigh uint32, ol *syscall.Overlapped) (err error) {
	r, _, err := procLockFileEx.Call(uintptr(h), uintptr(flags), uintptr(reserved), uintptr(locklow), uintptr(lockhigh), uintptr(unsafe.Pointer(ol)))
	if r == 0 {
		return err
	}
	return nil
}

func unlockFileEx(h syscall.Handle, reserved, locklow, lockhigh uint32, ol *syscall.Overlapped) (err error) {
	r, _, err := procUnlockFileEx.Call(uintptr(h), uintptr(reserved), uintptr(locklow), uintptr(lockhigh), uintptr(unsafe.Pointer(ol)), 0)
	if r == 0 {
		return err
	}
	return nil
}

func fdatasync(db *DB) error {
	return db.file.Sync()
}

func flock(db *DB, mode os.FileMode, exclusive bool, timeout time.Duration) error {

	f, err := os.OpenFile(db.path+lockExt, os.O_CREATE, mode)
	if err != nil {
		return err
	}
	db.lockfile = f

	var t time.Time
	for {

		if t.IsZero() {
			t = time.Now()
		} else if timeout > 0 && time.Since(t) > timeout {
			return ErrTimeout
		}

		var flag uint32 = flagLockFailImmediately
		if exclusive {
			flag |= flagLockExclusive
		}

		err := lockFileEx(syscall.Handle(db.lockfile.Fd()), flag, 0, 1, 0, &syscall.Overlapped{})
		if err == nil {
			return nil
		} else if err != errLockViolation {
			return err
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func funlock(db *DB) error {
	err := unlockFileEx(syscall.Handle(db.lockfile.Fd()), 0, 1, 0, &syscall.Overlapped{})
	db.lockfile.Close()
	os.Remove(db.path + lockExt)
	return err
}

func mmap(db *DB, sz int) error {
	if !db.readOnly {

		if err := db.file.Truncate(int64(sz)); err != nil {
			return fmt.Errorf("truncate: %s", err)
		}
	}

	sizelo := uint32(sz >> 32)
	sizehi := uint32(sz) & 0xffffffff
	h, errno := syscall.CreateFileMapping(syscall.Handle(db.file.Fd()), nil, syscall.PAGE_READONLY, sizelo, sizehi, nil)
	if h == 0 {
		return os.NewSyscallError("CreateFileMapping", errno)
	}

	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(sz))
	if addr == 0 {
		return os.NewSyscallError("MapViewOfFile", errno)
	}

	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		return os.NewSyscallError("CloseHandle", err)
	}

	db.data = ((*[maxMapSize]byte)(unsafe.Pointer(addr)))
	db.datasz = sz

	return nil
}

func munmap(db *DB) error {
	if db.data == nil {
		return nil
	}

	addr := (uintptr)(unsafe.Pointer(&db.data[0]))
	if err := syscall.UnmapViewOfFile(addr); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}
