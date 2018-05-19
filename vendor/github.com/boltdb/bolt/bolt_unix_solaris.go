package bolt

import (
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func flock(db *DB, mode os.FileMode, exclusive bool, timeout time.Duration) error {
	var t time.Time
	for {

		if t.IsZero() {
			t = time.Now()
		} else if timeout > 0 && time.Since(t) > timeout {
			return ErrTimeout
		}
		var lock syscall.Flock_t
		lock.Start = 0
		lock.Len = 0
		lock.Pid = 0
		lock.Whence = 0
		lock.Pid = 0
		if exclusive {
			lock.Type = syscall.F_WRLCK
		} else {
			lock.Type = syscall.F_RDLCK
		}
		err := syscall.FcntlFlock(db.file.Fd(), syscall.F_SETLK, &lock)
		if err == nil {
			return nil
		} else if err != syscall.EAGAIN {
			return err
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func funlock(db *DB) error {
	var lock syscall.Flock_t
	lock.Start = 0
	lock.Len = 0
	lock.Type = syscall.F_UNLCK
	lock.Whence = 0
	return syscall.FcntlFlock(uintptr(db.file.Fd()), syscall.F_SETLK, &lock)
}

func mmap(db *DB, sz int) error {

	b, err := unix.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED|db.MmapFlags)
	if err != nil {
		return err
	}

	if err := unix.Madvise(b, syscall.MADV_RANDOM); err != nil {
		return fmt.Errorf("madvise: %s", err)
	}

	db.dataref = b
	db.data = (*[maxMapSize]byte)(unsafe.Pointer(&b[0]))
	db.datasz = sz
	return nil
}

func munmap(db *DB) error {

	if db.dataref == nil {
		return nil
	}

	err := unix.Munmap(db.dataref)
	db.dataref = nil
	db.data = nil
	db.datasz = 0
	return err
}
