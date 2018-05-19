// Copyright 2009,2010 The Go Authors. All rights reserved.

package unix

import (
	errorspkg "errors"
	"syscall"
	"unsafe"
)

const ImplementsGetwd = true

func Getwd() (string, error) {
	buf := make([]byte, 2048)
	attrs, err := getAttrList(".", attrList{CommonAttr: attrCmnFullpath}, buf, 0)
	if err == nil && len(attrs) == 1 && len(attrs[0]) >= 2 {
		wd := string(attrs[0])

		if wd[0] == '/' && wd[len(wd)-1] == 0 {
			return wd[:len(wd)-1], nil
		}
	}

	return "", ENOTSUP
}

type SockaddrDatalink struct {
	Len    uint8
	Family uint8
	Index  uint16
	Type   uint8
	Nlen   uint8
	Alen   uint8
	Slen   uint8
	Data   [12]int8
	raw    RawSockaddrDatalink
}

func nametomib(name string) (mib []_C_int, err error) {
	const siz = unsafe.Sizeof(mib[0])

	var buf [CTL_MAXNAME + 2]_C_int
	n := uintptr(CTL_MAXNAME) * siz

	p := (*byte)(unsafe.Pointer(&buf[0]))
	bytes, err := ByteSliceFromString(name)
	if err != nil {
		return nil, err
	}

	if err = sysctl([]_C_int{0, 3}, p, &n, &bytes[0], uintptr(len(name))); err != nil {
		return nil, err
	}
	return buf[0 : n/siz], nil
}

func direntIno(buf []byte) (uint64, bool) {
	return readInt(buf, unsafe.Offsetof(Dirent{}.Ino), unsafe.Sizeof(Dirent{}.Ino))
}

func direntReclen(buf []byte) (uint64, bool) {
	return readInt(buf, unsafe.Offsetof(Dirent{}.Reclen), unsafe.Sizeof(Dirent{}.Reclen))
}

func direntNamlen(buf []byte) (uint64, bool) {
	return readInt(buf, unsafe.Offsetof(Dirent{}.Namlen), unsafe.Sizeof(Dirent{}.Namlen))
}

func PtraceAttach(pid int) (err error) { return ptrace(PT_ATTACH, pid, 0, 0) }
func PtraceDetach(pid int) (err error) { return ptrace(PT_DETACH, pid, 0, 0) }

const (
	attrBitMapCount = 5
	attrCmnFullpath = 0x08000000
)

type attrList struct {
	bitmapCount uint16
	_           uint16
	CommonAttr  uint32
	VolAttr     uint32
	DirAttr     uint32
	FileAttr    uint32
	Forkattr    uint32
}

func getAttrList(path string, attrList attrList, attrBuf []byte, options uint) (attrs [][]byte, err error) {
	if len(attrBuf) < 4 {
		return nil, errorspkg.New("attrBuf too small")
	}
	attrList.bitmapCount = attrBitMapCount

	var _p0 *byte
	_p0, err = BytePtrFromString(path)
	if err != nil {
		return nil, err
	}

	_, _, e1 := Syscall6(
		SYS_GETATTRLIST,
		uintptr(unsafe.Pointer(_p0)),
		uintptr(unsafe.Pointer(&attrList)),
		uintptr(unsafe.Pointer(&attrBuf[0])),
		uintptr(len(attrBuf)),
		uintptr(options),
		0,
	)
	if e1 != 0 {
		return nil, e1
	}
	size := *(*uint32)(unsafe.Pointer(&attrBuf[0]))

	dat := attrBuf
	if int(size) < len(attrBuf) {
		dat = dat[:size]
	}
	dat = dat[4:] // remove length prefix

	for i := uint32(0); int(i) < len(dat); {
		header := dat[i:]
		if len(header) < 8 {
			return attrs, errorspkg.New("truncated attribute header")
		}
		datOff := *(*int32)(unsafe.Pointer(&header[0]))
		attrLen := *(*uint32)(unsafe.Pointer(&header[4]))
		if datOff < 0 || uint32(datOff)+attrLen > uint32(len(dat)) {
			return attrs, errorspkg.New("truncated results; attrBuf too small")
		}
		end := uint32(datOff) + attrLen
		attrs = append(attrs, dat[datOff:end])
		i = end
		if r := i % 4; r != 0 {
			i += (4 - r)
		}
	}
	return
}

func Pipe(p []int) (err error) {
	if len(p) != 2 {
		return EINVAL
	}
	p[0], p[1], err = pipe()
	return
}

func Getfsstat(buf []Statfs_t, flags int) (n int, err error) {
	var _p0 unsafe.Pointer
	var bufsize uintptr
	if len(buf) > 0 {
		_p0 = unsafe.Pointer(&buf[0])
		bufsize = unsafe.Sizeof(Statfs_t{}) * uintptr(len(buf))
	}
	r0, _, e1 := Syscall(SYS_GETFSSTAT64, uintptr(_p0), bufsize, uintptr(flags))
	n = int(r0)
	if e1 != 0 {
		err = e1
	}
	return
}

/*
 * Wrapped
 */

func Kill(pid int, signum syscall.Signal) (err error) { return kill(pid, int(signum), 1) }

/*
 * Exposed directly
 */

/*
 * Unimplemented
 */
// Sigaltstack
