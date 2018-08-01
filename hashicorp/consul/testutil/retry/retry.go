// Package retry provides support for repeating operations in tests.
//
//
//
package retry

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"
)

type Failer interface {
	Log(args ...interface{})

	FailNow()
}

type R struct {
	fail   bool
	output []string
}

func (r *R) FailNow() {
	r.fail = true
	runtime.Goexit()
}

func (r *R) Fatal(args ...interface{}) {
	r.log(fmt.Sprint(args...))
	r.FailNow()
}

func (r *R) Fatalf(format string, args ...interface{}) {
	r.log(fmt.Sprintf(format, args...))
	r.FailNow()
}

func (r *R) Error(args ...interface{}) {
	r.log(fmt.Sprint(args...))
	r.fail = true
}

func (r *R) Check(err error) {
	if err != nil {
		r.log(err.Error())
		r.FailNow()
	}
}

func (r *R) log(s string) {
	r.output = append(r.output, decorate(s))
}

func decorate(s string) string {
	_, file, line, ok := runtime.Caller(3)
	if ok {
		n := strings.LastIndex(file, "/")
		if n >= 0 {
			file = file[n+1:]
		}
	} else {
		file = "???"
		line = 1
	}
	return fmt.Sprintf("%s:%d: %s", file, line, s)
}

func Run(t Failer, f func(r *R)) {
	run(DefaultFailer(), t, f)
}

func RunWith(r Retryer, t Failer, f func(r *R)) {
	run(r, t, f)
}

func dedup(a []string) string {
	if len(a) == 0 {
		return ""
	}
	m := map[string]int{}
	for _, s := range a {
		m[s] = m[s] + 1
	}
	var b bytes.Buffer
	for _, s := range a {
		if _, ok := m[s]; ok {
			b.WriteString(s)
			b.WriteRune('\n')
			delete(m, s)
		}
	}
	return string(b.Bytes())
}

func run(r Retryer, t Failer, f func(r *R)) {
	rr := &R{}
	fail := func() {
		out := dedup(rr.output)
		if out != "" {
			t.Log(out)
		}
		t.FailNow()
	}
	for r.NextOr(fail) {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			f(rr)
		}()
		wg.Wait()
		if rr.fail {
			rr.fail = false
			continue
		}
		break
	}
}

func DefaultFailer() *Timer {
	return &Timer{Timeout: 7 * time.Second, Wait: 25 * time.Millisecond}
}

func TwoSeconds() *Timer {
	return &Timer{Timeout: 2 * time.Second, Wait: 25 * time.Millisecond}
}

func ThreeTimes() *Counter {
	return &Counter{Count: 3, Wait: 25 * time.Millisecond}
}

type Retryer interface {
	NextOr(fail func()) bool
}

type Counter struct {
	Count int
	Wait  time.Duration

	count int
}

func (r *Counter) NextOr(fail func()) bool {
	if r.count == r.Count {
		fail()
		return false
	}
	if r.count > 0 {
		time.Sleep(r.Wait)
	}
	r.count++
	return true
}

type Timer struct {
	Timeout time.Duration
	Wait    time.Duration

	stop time.Time
}

func (r *Timer) NextOr(fail func()) bool {
	if r.stop.IsZero() {
		r.stop = time.Now().Add(r.Timeout)
		return true
	}
	if time.Now().After(r.stop) {
		fail()
		return false
	}
	time.Sleep(r.Wait)
	return true
}
