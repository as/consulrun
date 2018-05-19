// Copyright 2009 The Go Authors. All rights reserved.

// +build amd64,linux

package unix

func Gettimeofday(tv *Timeval) (err error) {
	errno := gettimeofday(tv)
	if errno != 0 {
		return errno
	}
	return nil
}

func Getpagesize() int { return 4096 }

func Time(t *Time_t) (tt Time_t, err error) {
	var tv Timeval
	errno := gettimeofday(&tv)
	if errno != 0 {
		return 0, errno
	}
	if t != nil {
		*t = Time_t(tv.Sec)
	}
	return Time_t(tv.Sec), nil
}

func TimespecToNsec(ts Timespec) int64 { return int64(ts.Sec)*1e9 + int64(ts.Nsec) }

func NsecToTimespec(nsec int64) (ts Timespec) {
	ts.Sec = nsec / 1e9
	ts.Nsec = nsec % 1e9
	return
}

func NsecToTimeval(nsec int64) (tv Timeval) {
	nsec += 999 // round up to microsecond
	tv.Sec = nsec / 1e9
	tv.Usec = nsec % 1e9 / 1e3
	return
}

func Pipe(p []int) (err error) {
	if len(p) != 2 {
		return EINVAL
	}
	var pp [2]_C_int
	err = pipe(&pp)
	p[0] = int(pp[0])
	p[1] = int(pp[1])
	return
}

func Pipe2(p []int, flags int) (err error) {
	if len(p) != 2 {
		return EINVAL
	}
	var pp [2]_C_int
	err = pipe2(&pp, flags)
	p[0] = int(pp[0])
	p[1] = int(pp[1])
	return
}

func (r *PtraceRegs) PC() uint64 { return r.Rip }

func (r *PtraceRegs) SetPC(pc uint64) { r.Rip = pc }

func (iov *Iovec) SetLen(length int) {
	iov.Len = uint64(length)
}

func (msghdr *Msghdr) SetControllen(length int) {
	msghdr.Controllen = uint64(length)
}

func (cmsg *Cmsghdr) SetLen(length int) {
	cmsg.Len = uint64(length)
}

func Poll(fds []PollFd, timeout int) (n int, err error) {
	if len(fds) == 0 {
		return poll(nil, 0, timeout)
	}
	return poll(&fds[0], len(fds), timeout)
}
