// +build !go1.9

package testing

import (
	"fmt"
	"log"
)

//
type T interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Fail()
	FailNow()
	Failed() bool
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Log(args ...interface{})
	Logf(format string, args ...interface{})
	Name() string
	Skip(args ...interface{})
	SkipNow()
	Skipf(format string, args ...interface{})
	Skipped() bool
}

type RuntimeT struct {
	failed bool
}

func (t *RuntimeT) Error(args ...interface{}) {
	log.Println(fmt.Sprintln(args...))
	t.Fail()
}

func (t *RuntimeT) Errorf(format string, args ...interface{}) {
	log.Println(fmt.Sprintf(format, args...))
	t.Fail()
}

func (t *RuntimeT) Fatal(args ...interface{}) {
	log.Println(fmt.Sprintln(args...))
	t.FailNow()
}

func (t *RuntimeT) Fatalf(format string, args ...interface{}) {
	log.Println(fmt.Sprintf(format, args...))
	t.FailNow()
}

func (t *RuntimeT) Fail() {
	t.failed = true
}

func (t *RuntimeT) FailNow() {
	panic("testing.T failed, see logs for output (if any)")
}

func (t *RuntimeT) Failed() bool {
	return t.failed
}

func (t *RuntimeT) Log(args ...interface{}) {
	log.Println(fmt.Sprintln(args...))
}

func (t *RuntimeT) Logf(format string, args ...interface{}) {
	log.Println(fmt.Sprintf(format, args...))
}

func (t *RuntimeT) Name() string                             { return "" }
func (t *RuntimeT) Skip(args ...interface{})                 {}
func (t *RuntimeT) SkipNow()                                 {}
func (t *RuntimeT) Skipf(format string, args ...interface{}) {}
func (t *RuntimeT) Skipped() bool                            { return false }
