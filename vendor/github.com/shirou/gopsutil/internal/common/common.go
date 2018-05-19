package common

//
import (
	"bufio"
	"bytes"
	"errors"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var (
	Timeout    = 3 * time.Second
	ErrTimeout = errors.New("Command timed out.")
)

type Invoker interface {
	Command(string, ...string) ([]byte, error)
}

type Invoke struct{}

func (i Invoke) Command(name string, arg ...string) ([]byte, error) {
	cmd := exec.Command(name, arg...)
	return CombinedOutputTimeout(cmd, Timeout)
}

type FakeInvoke struct {
	CommandExpectedDir string // CommandExpectedDir specifies dir which includes expected outputs.
	Suffix             string // Suffix species expected file name suffix such as "fail"
	Error              error  // If Error specfied, return the error.
}

func (i FakeInvoke) Command(name string, arg ...string) ([]byte, error) {
	if i.Error != nil {
		return []byte{}, i.Error
	}

	arch := runtime.GOOS

	fname := strings.Join(append([]string{name}, arg...), "")
	fname = url.QueryEscape(fname)
	var dir string
	if i.CommandExpectedDir == "" {
		dir = "expected"
	} else {
		dir = i.CommandExpectedDir
	}
	fpath := path.Join(dir, arch, fname)
	if i.Suffix != "" {
		fpath += "_" + i.Suffix
	}
	if PathExists(fpath) {
		return ioutil.ReadFile(fpath)
	}
	return exec.Command(name, arg...).Output()
}

var ErrNotImplementedError = errors.New("not implemented yet")

func ReadLines(filename string) ([]string, error) {
	return ReadLinesOffsetN(filename, 0, -1)
}

func ReadLinesOffsetN(filename string, offset uint, n int) ([]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return []string{""}, err
	}
	defer f.Close()

	var ret []string

	r := bufio.NewReader(f)
	for i := 0; i < n+int(offset) || n < 0; i++ {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		if i < int(offset) {
			continue
		}
		ret = append(ret, strings.Trim(line, "\n"))
	}

	return ret, nil
}

func IntToString(orig []int8) string {
	ret := make([]byte, len(orig))
	size := -1
	for i, o := range orig {
		if o == 0 {
			size = i
			break
		}
		ret[i] = byte(o)
	}
	if size == -1 {
		size = len(orig)
	}

	return string(ret[0:size])
}

func UintToString(orig []uint8) string {
	ret := make([]byte, len(orig))
	size := -1
	for i, o := range orig {
		if o == 0 {
			size = i
			break
		}
		ret[i] = byte(o)
	}
	if size == -1 {
		size = len(orig)
	}

	return string(ret[0:size])
}

func ByteToString(orig []byte) string {
	n := -1
	l := -1
	for i, b := range orig {

		if l == -1 && b == 0 {
			continue
		}
		if l == -1 {
			l = i
		}

		if b == 0 {
			break
		}
		n = i + 1
	}
	if n == -1 {
		return string(orig)
	}
	return string(orig[l:n])
}

func ReadInts(filename string) ([]int64, error) {
	f, err := os.Open(filename)
	if err != nil {
		return []int64{}, err
	}
	defer f.Close()

	var ret []int64

	r := bufio.NewReader(f)

	line, err := r.ReadString('\n')
	if err != nil {
		return []int64{}, err
	}

	i, err := strconv.ParseInt(strings.Trim(line, "\n"), 10, 32)
	if err != nil {
		return []int64{}, err
	}
	ret = append(ret, i)

	return ret, nil
}

func mustParseInt32(val string) int32 {
	vv, _ := strconv.ParseInt(val, 10, 32)
	return int32(vv)
}

func mustParseUint64(val string) uint64 {
	vv, _ := strconv.ParseInt(val, 10, 64)
	return uint64(vv)
}

func mustParseFloat64(val string) float64 {
	vv, _ := strconv.ParseFloat(val, 64)
	return vv
}

func StringsHas(target []string, src string) bool {
	for _, t := range target {
		if strings.TrimSpace(t) == src {
			return true
		}
	}
	return false
}

func StringsContains(target []string, src string) bool {
	for _, t := range target {
		if strings.Contains(t, src) {
			return true
		}
	}
	return false
}

func IntContains(target []int, src int) bool {
	for _, t := range target {
		if src == t {
			return true
		}
	}
	return false
}

func attributes(m interface{}) map[string]reflect.Type {
	typ := reflect.TypeOf(m)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	attrs := make(map[string]reflect.Type)
	if typ.Kind() != reflect.Struct {
		return nil
	}

	for i := 0; i < typ.NumField(); i++ {
		p := typ.Field(i)
		if !p.Anonymous {
			attrs[p.Name] = p.Type
		}
	}

	return attrs
}

func PathExists(filename string) bool {
	if _, err := os.Stat(filename); err == nil {
		return true
	}
	return false
}

func GetEnv(key string, dfault string, combineWith ...string) string {
	value := os.Getenv(key)
	if value == "" {
		value = dfault
	}

	switch len(combineWith) {
	case 0:
		return value
	case 1:
		return filepath.Join(value, combineWith[0])
	default:
		all := make([]string, len(combineWith)+1)
		all[0] = value
		copy(all[1:], combineWith)
		return filepath.Join(all...)
	}
	panic("invalid switch case")
}

func HostProc(combineWith ...string) string {
	return GetEnv("HOST_PROC", "/proc", combineWith...)
}

func HostSys(combineWith ...string) string {
	return GetEnv("HOST_SYS", "/sys", combineWith...)
}

func HostEtc(combineWith ...string) string {
	return GetEnv("HOST_ETC", "/etc", combineWith...)
}

func CombinedOutputTimeout(c *exec.Cmd, timeout time.Duration) ([]byte, error) {
	var b bytes.Buffer
	c.Stdout = &b
	c.Stderr = &b
	if err := c.Start(); err != nil {
		return nil, err
	}
	err := WaitTimeout(c, timeout)
	return b.Bytes(), err
}

func WaitTimeout(c *exec.Cmd, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	done := make(chan error)
	go func() { done <- c.Wait() }()
	select {
	case err := <-done:
		timer.Stop()
		return err
	case <-timer.C:
		if err := c.Process.Kill(); err != nil {
			log.Printf("FATAL error killing process: %s", err)
			return err
		}

		<-done
		return ErrTimeout
	}
}

func Pipeline(cmds ...*exec.Cmd) ([]byte, []byte, error) {

	if len(cmds) < 1 {
		return nil, nil, nil
	}

	var output bytes.Buffer
	var stderr bytes.Buffer

	last := len(cmds) - 1
	for i, cmd := range cmds[:last] {
		var err error

		if cmds[i+1].Stdin, err = cmd.StdoutPipe(); err != nil {
			return nil, nil, err
		}

		cmd.Stderr = &stderr
	}

	cmds[last].Stdout, cmds[last].Stderr = &output, &stderr

	for _, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			return output.Bytes(), stderr.Bytes(), err
		}
	}

	for _, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			return output.Bytes(), stderr.Bytes(), err
		}
	}

	return output.Bytes(), stderr.Bytes(), nil
}
