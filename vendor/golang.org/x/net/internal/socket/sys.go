// Copyright 2017 The Go Authors. All rights reserved.

package socket

import (
	"encoding/binary"
	"unsafe"
)

var (
	NativeEndian binary.ByteOrder

	kernelAlign int
)

func init() {
	i := uint32(1)
	b := (*[4]byte)(unsafe.Pointer(&i))
	if b[0] == 1 {
		NativeEndian = binary.LittleEndian
	} else {
		NativeEndian = binary.BigEndian
	}
	kernelAlign = probeProtocolStack()
}

func roundup(l int) int {
	return (l + kernelAlign - 1) & ^(kernelAlign - 1)
}
