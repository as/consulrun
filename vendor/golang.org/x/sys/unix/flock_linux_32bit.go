// +build linux,386 linux,arm linux,mips linux,mipsle

package unix

func init() {

	fcntl64Syscall = SYS_FCNTL64
}
