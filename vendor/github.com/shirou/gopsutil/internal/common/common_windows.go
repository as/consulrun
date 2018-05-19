// +build windows

package common

import (
	"syscall"
	"unsafe"
)

type PDH_FMT_COUNTERVALUE_DOUBLE struct {
	CStatus     uint32
	DoubleValue float64
}

type PDH_FMT_COUNTERVALUE_LARGE struct {
	CStatus    uint32
	LargeValue int64
}

type PDH_FMT_COUNTERVALUE_LONG struct {
	CStatus   uint32
	LongValue int32
	padding   [4]byte
}

const (
	ERROR_SUCCESS        = 0
	ERROR_FILE_NOT_FOUND = 2
	DRIVE_REMOVABLE      = 2
	DRIVE_FIXED          = 3
	HKEY_LOCAL_MACHINE   = 0x80000002
	RRF_RT_REG_SZ        = 0x00000002
	RRF_RT_REG_DWORD     = 0x00000010
	PDH_FMT_LONG         = 0x00000100
	PDH_FMT_DOUBLE       = 0x00000200
	PDH_FMT_LARGE        = 0x00000400
	PDH_INVALID_DATA     = 0xc0000bc6
	PDH_INVALID_HANDLE   = 0xC0000bbc
	PDH_NO_DATA          = 0x800007d5
)

var (
	Modkernel32 = syscall.NewLazyDLL("kernel32.dll")
	ModNt       = syscall.NewLazyDLL("ntdll.dll")
	ModPdh      = syscall.NewLazyDLL("pdh.dll")

	ProcGetSystemTimes           = Modkernel32.NewProc("GetSystemTimes")
	ProcNtQuerySystemInformation = ModNt.NewProc("NtQuerySystemInformation")
	PdhOpenQuery                 = ModPdh.NewProc("PdhOpenQuery")
	PdhAddCounter                = ModPdh.NewProc("PdhAddCounterW")
	PdhCollectQueryData          = ModPdh.NewProc("PdhCollectQueryData")
	PdhGetFormattedCounterValue  = ModPdh.NewProc("PdhGetFormattedCounterValue")
	PdhCloseQuery                = ModPdh.NewProc("PdhCloseQuery")
)

type FILETIME struct {
	DwLowDateTime  uint32
	DwHighDateTime uint32
}

func BytePtrToString(p *uint8) string {
	a := (*[10000]uint8)(unsafe.Pointer(p))
	i := 0
	for a[i] != 0 {
		i++
	}
	return string(a[:i])
}

type CounterInfo struct {
	PostName    string
	CounterName string
	Counter     syscall.Handle
}

func CreateQuery() (syscall.Handle, error) {
	var query syscall.Handle
	r, _, err := PdhOpenQuery.Call(0, 0, uintptr(unsafe.Pointer(&query)))
	if r != 0 {
		return 0, err
	}
	return query, nil
}

func CreateCounter(query syscall.Handle, pname, cname string) (*CounterInfo, error) {
	var counter syscall.Handle
	r, _, err := PdhAddCounter.Call(
		uintptr(query),
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(cname))),
		0,
		uintptr(unsafe.Pointer(&counter)))
	if r != 0 {
		return nil, err
	}
	return &CounterInfo{
		PostName:    pname,
		CounterName: cname,
		Counter:     counter,
	}, nil
}
