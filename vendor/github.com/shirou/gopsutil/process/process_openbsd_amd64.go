// Created by cgo -godefs - DO NOT EDIT

package process

const (
	CTLKern          = 1
	KernProc         = 66
	KernProcAll      = 0
	KernProcPID      = 1
	KernProcProc     = 8
	KernProcPathname = 12
	KernProcArgs     = 55
	KernProcArgv     = 1
	KernProcEnv      = 3
)

const (
	ArgMax = 256 * 1024
)

const (
	sizeofPtr      = 0x8
	sizeofShort    = 0x2
	sizeofInt      = 0x4
	sizeofLong     = 0x8
	sizeofLongLong = 0x8
)

const (
	sizeOfKinfoVmentry = 0x50
	sizeOfKinfoProc    = 0x268
)

const (
	SIDL    = 1
	SRUN    = 2
	SSLEEP  = 3
	SSTOP   = 4
	SZOMB   = 5
	SDEAD   = 6
	SONPROC = 7
)

type (
	_C_short     int16
	_C_int       int32
	_C_long      int64
	_C_long_long int64
)

type Timespec struct {
	Sec  int64
	Nsec int64
}

type Timeval struct {
	Sec  int64
	Usec int64
}

type Rusage struct {
	Utime    Timeval
	Stime    Timeval
	Maxrss   int64
	Ixrss    int64
	Idrss    int64
	Isrss    int64
	Minflt   int64
	Majflt   int64
	Nswap    int64
	Inblock  int64
	Oublock  int64
	Msgsnd   int64
	Msgrcv   int64
	Nsignals int64
	Nvcsw    int64
	Nivcsw   int64
}

type Rlimit struct {
	Cur uint64
	Max uint64
}

type KinfoProc struct {
	Forw         uint64
	Back         uint64
	Paddr        uint64
	Addr         uint64
	Fd           uint64
	Stats        uint64
	Limit        uint64
	Vmspace      uint64
	Sigacts      uint64
	Sess         uint64
	Tsess        uint64
	Ru           uint64
	Eflag        int32
	Exitsig      int32
	Flag         int32
	Pid          int32
	Ppid         int32
	Sid          int32
	X_pgid       int32
	Tpgid        int32
	Uid          uint32
	Ruid         uint32
	Gid          uint32
	Rgid         uint32
	Groups       [16]uint32
	Ngroups      int16
	Jobc         int16
	Tdev         uint32
	Estcpu       uint32
	Rtime_sec    uint32
	Rtime_usec   uint32
	Cpticks      int32
	Pctcpu       uint32
	Swtime       uint32
	Slptime      uint32
	Schedflags   int32
	Uticks       uint64
	Sticks       uint64
	Iticks       uint64
	Tracep       uint64
	Traceflag    int32
	Holdcnt      int32
	Siglist      int32
	Sigmask      uint32
	Sigignore    uint32
	Sigcatch     uint32
	Stat         int8
	Priority     uint8
	Usrpri       uint8
	Nice         uint8
	Xstat        uint16
	Acflag       uint16
	Comm         [24]int8
	Wmesg        [8]int8
	Wchan        uint64
	Login        [32]int8
	Vm_rssize    int32
	Vm_tsize     int32
	Vm_dsize     int32
	Vm_ssize     int32
	Uvalid       int64
	Ustart_sec   uint64
	Ustart_usec  uint32
	Uutime_sec   uint32
	Uutime_usec  uint32
	Ustime_sec   uint32
	Ustime_usec  uint32
	Pad_cgo_0    [4]byte
	Uru_maxrss   uint64
	Uru_ixrss    uint64
	Uru_idrss    uint64
	Uru_isrss    uint64
	Uru_minflt   uint64
	Uru_majflt   uint64
	Uru_nswap    uint64
	Uru_inblock  uint64
	Uru_oublock  uint64
	Uru_msgsnd   uint64
	Uru_msgrcv   uint64
	Uru_nsignals uint64
	Uru_nvcsw    uint64
	Uru_nivcsw   uint64
	Uctime_sec   uint32
	Uctime_usec  uint32
	Psflags      int32
	Spare        int32
	Svuid        uint32
	Svgid        uint32
	Emul         [8]int8
	Rlim_rss_cur uint64
	Cpuid        uint64
	Vm_map_size  uint64
	Tid          int32
	Rtableid     uint32
}

type Priority struct{}

type KinfoVmentry struct {
	Start          uint64
	End            uint64
	Guard          uint64
	Fspace         uint64
	Fspace_augment uint64
	Offset         uint64
	Wired_count    int32
	Etype          int32
	Protection     int32
	Max_protection int32
	Advice         int32
	Inheritance    int32
	Flags          uint8
	Pad_cgo_0      [7]byte
}