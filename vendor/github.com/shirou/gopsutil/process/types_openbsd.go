// +build ignore

/*
Input to cgo -godefs.
*/

package process

/*
#include <sys/types.h>
#include <sys/sysctl.h>
#include <sys/user.h>

enum {
	sizeofPtr = sizeof(void*),
};


*/
import "C"

const (
	CTLKern          = 1  // "high kernel": proc, limits
	KernProc         = 66 // struct: process entries
	KernProcAll      = 0
	KernProcPID      = 1  // by process id
	KernProcProc     = 8  // only return procs
	KernProcPathname = 12 // path to executable
	KernProcArgs     = 55 // get/set arguments/proctitle
	KernProcArgv     = 1
	KernProcEnv      = 3
)

const (
	ArgMax = 256 * 1024 // sys/syslimits.h:#define  ARG_MAX
)

const (
	sizeofPtr      = C.sizeofPtr
	sizeofShort    = C.sizeof_short
	sizeofInt      = C.sizeof_int
	sizeofLong     = C.sizeof_long
	sizeofLongLong = C.sizeof_longlong
)

const (
	sizeOfKinfoVmentry = C.sizeof_struct_kinfo_vmentry
	sizeOfKinfoProc    = C.sizeof_struct_kinfo_proc
)

const (
	SIDL    = 1 /* Process being created by fork. */
	SRUN    = 2 /* Currently runnable. */
	SSLEEP  = 3 /* Sleeping on an address. */
	SSTOP   = 4 /* Process debugging or suspension. */
	SZOMB   = 5 /* Awaiting collection by parent. */
	SDEAD   = 6 /* Thread is almost gone */
	SONPROC = 7 /* Thread is currently on a CPU. */
)

type (
	_C_short     C.short
	_C_int       C.int
	_C_long      C.long
	_C_long_long C.longlong
)

type Timespec C.struct_timespec

type Timeval C.struct_timeval

type Rusage C.struct_rusage

type Rlimit C.struct_rlimit

type KinfoProc C.struct_kinfo_proc

type Priority C.struct_priority

type KinfoVmentry C.struct_kinfo_vmentry
