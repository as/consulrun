package bolt

import "unsafe"

const maxMapSize = 0x7FFFFFFF // 2GB

const maxAllocSize = 0xFFFFFFF

var brokenUnaligned bool

func init() {

	raw := [6]byte{0xfe, 0xef, 0x11, 0x22, 0x22, 0x11}
	val := *(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(&raw)) + 2))

	brokenUnaligned = val != 0x11222211
}
