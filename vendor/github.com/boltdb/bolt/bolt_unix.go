// +build !windows,!plan9,!solaris

package bolt

import (
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"
)

func flock(db *DB, mode os.FileMode, exclusive bool, timeout time.Duration) error {
	var t time.Time
	for {

		if t.IsZero() {
			t = time.Now()
		} else if timeout > 0 && time.Since(t) > timeout {
			return ErrTimeout
		}
		flag := syscall.LOCK_SH
		if exclusive {
			flag = syscall.LOCK_EX
		}

		err := syscall.Flock(int(db.file.Fd()), flag|syscall.LOCK_NB)
		if err == nil {
			return nil
		} else if err != syscall.EWOULDBLOCK {
			return err
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func funlock(db *DB) error {
	return syscall.Flock(int(db.file.Fd()), syscall.LOCK_UN)
}

func mmap(db *DB, sz int) error {

	b, err := syscall.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED|db.MmapFlags)
	if err != nil {
		return err
	}

	if err := madvise(b, syscall.MADV_RANDOM); err != nil {
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

	err := syscall.Munmap(db.dataref)
	db.dataref = nil
	db.data = nil
	db.datasz = 0
	return err
}

func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}
