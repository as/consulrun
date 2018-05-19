// based on https://code.google.com/p/gopass
//

// +build darwin freebsd linux netbsd openbsd solaris

package speakeasy

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

const sttyArg0 = "/bin/stty"

var (
	sttyArgvEOff = []string{"stty", "-echo"}
	sttyArgvEOn  = []string{"stty", "echo"}
)

func getPassword() (password string, err error) {
	sig := make(chan os.Signal, 10)
	brk := make(chan bool)

	fd := []uintptr{os.Stdin.Fd(), os.Stdout.Fd(), os.Stderr.Fd()}

	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT,
		syscall.SIGTERM)
	go catchSignal(fd, sig, brk)

	pid, err := echoOff(fd)
	if err != nil {
		return "", err
	}

	defer signal.Stop(sig)
	defer close(brk)
	defer echoOn(fd)

	syscall.Wait4(pid, nil, 0, nil)

	line, err := readline()
	if err == nil {
		password = strings.TrimSpace(line)
	} else {
		err = fmt.Errorf("failed during password entry: %s", err)
	}

	return password, err
}

func echoOff(fd []uintptr) (int, error) {
	pid, err := syscall.ForkExec(sttyArg0, sttyArgvEOff, &syscall.ProcAttr{Dir: "", Files: fd})
	if err != nil {
		return 0, fmt.Errorf("failed turning off console echo for password entry:\n\t%s", err)
	}
	return pid, nil
}

func echoOn(fd []uintptr) {

	pid, e := syscall.ForkExec(sttyArg0, sttyArgvEOn, &syscall.ProcAttr{Dir: "", Files: fd})
	if e == nil {
		syscall.Wait4(pid, nil, 0, nil)
	}
}

func catchSignal(fd []uintptr, sig chan os.Signal, brk chan bool) {
	select {
	case <-sig:
		echoOn(fd)
		os.Exit(-1)
	case <-brk:
	}
}
