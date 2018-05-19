// Copyright 2009,2010 The Go Authors. All rights reserved.

package unix

import (
	"syscall"
	"unsafe"
)

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

func Syscall9(trap, a1, a2, a3, a4, a5, a6, a7, a8, a9 uintptr) (r1, r2 uintptr, err syscall.Errno)

func sysctlNodes(mib []_C_int) (nodes []Sysctlnode, err error) {
	var olen uintptr

	mib = append(mib, CTL_QUERY)
	qnode := Sysctlnode{Flags: SYSCTL_VERS_1}
	qp := (*byte)(unsafe.Pointer(&qnode))
	sz := unsafe.Sizeof(qnode)
	if err = sysctl(mib, nil, &olen, qp, sz); err != nil {
		return nil, err
	}

	nodes = make([]Sysctlnode, olen/sz)
	np := (*byte)(unsafe.Pointer(&nodes[0]))
	if err = sysctl(mib, np, &olen, qp, sz); err != nil {
		return nil, err
	}

	return nodes, nil
}

func nametomib(name string) (mib []_C_int, err error) {

	var parts []string
	last := 0
	for i := 0; i < len(name); i++ {
		if name[i] == '.' {
			parts = append(parts, name[last:i])
			last = i + 1
		}
	}
	parts = append(parts, name[last:])

	for partno, part := range parts {
		nodes, err := sysctlNodes(mib)
		if err != nil {
			return nil, err
		}
		for _, node := range nodes {
			n := make([]byte, 0)
			for i := range node.Name {
				if node.Name[i] != 0 {
					n = append(n, byte(node.Name[i]))
				}
			}
			if string(n) == part {
				mib = append(mib, _C_int(node.Num))
				break
			}
		}
		if len(mib) != partno+1 {
			return nil, EINVAL
		}
	}

	return mib, nil
}

func direntIno(buf []byte) (uint64, bool) {
	return readInt(buf, unsafe.Offsetof(Dirent{}.Fileno), unsafe.Sizeof(Dirent{}.Fileno))
}

func direntReclen(buf []byte) (uint64, bool) {
	return readInt(buf, unsafe.Offsetof(Dirent{}.Reclen), unsafe.Sizeof(Dirent{}.Reclen))
}

func direntNamlen(buf []byte) (uint64, bool) {
	return readInt(buf, unsafe.Offsetof(Dirent{}.Namlen), unsafe.Sizeof(Dirent{}.Namlen))
}

func Pipe(p []int) (err error) {
	if len(p) != 2 {
		return EINVAL
	}
	p[0], p[1], err = pipe()
	return
}

func Getdirentries(fd int, buf []byte, basep *uintptr) (n int, err error) {
	return getdents(fd, buf)
}

func sendfile(outfd int, infd int, offset *int64, count int) (written int, err error) {
	return -1, ENOSYS
}

/*
 * Exposed directly
 */

/*
 * Unimplemented
 */
// __sigaltstack14
// compat_13_sigaltstack13
// compat_43_osigstack
// sa_stacks
