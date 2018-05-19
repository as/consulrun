package consul

import (
	"testing"
	"time"
)

func TestSessionTimers(t *testing.T) {
	m := NewSessionTimers()
	ch := make(chan int)
	newTm := func(d time.Duration) *time.Timer {
		return time.AfterFunc(d, func() { ch <- 1 })
	}

	waitForTimer := func() {
		select {
		case <-ch:
			return
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timer did not fire")
		}
	}

	if got, want := m.Get("foo"), (*time.Timer)(nil); got != want {
		t.Fatalf("got %v want %v", got, want)
	}

	tm := newTm(time.Millisecond)
	m.Set("foo", tm)
	if got, want := m.Len(), 1; got != want {
		t.Fatalf("got len %d want %d", got, want)
	}
	if got, want := m.Get("foo"), tm; got != want {
		t.Fatalf("got %v want %v", got, want)
	}
	m.Set("foo", nil)
	if got, want := m.Get("foo"), (*time.Timer)(nil); got != want {
		t.Fatalf("got %v want %v", got, want)
	}
	waitForTimer()

	tm = newTm(time.Millisecond)
	m.Set("foo", tm)
	if got, want := m.Get("foo"), tm; got != want {
		t.Fatalf("got %v want %v", got, want)
	}
	m.Del("foo")
	if got, want := m.Len(), 0; got != want {
		t.Fatalf("got len %d want %d", got, want)
	}
	waitForTimer()

	m.ResetOrCreate("foo", time.Millisecond, func() { ch <- 1 })
	if got, want := m.Len(), 1; got != want {
		t.Fatalf("got len %d want %d", got, want)
	}
	waitForTimer()

	if got, want := m.Len(), 1; got != want {
		t.Fatalf("got len %d want %d", got, want)
	}

	m.ResetOrCreate("foo", time.Millisecond, nil)
	waitForTimer()

	m.ResetOrCreate("foo", 20*time.Millisecond, func() { ch <- 1 })
	m.Stop("foo")
	select {
	case <-ch:
		t.Fatal("timer fired although it shouldn't")
	case <-time.After(100 * time.Millisecond):

	}

	m.Stop("foo")

	if got, want := m.Len(), 0; got != want {
		t.Fatalf("got len %d want %d", got, want)
	}

	m.ResetOrCreate("foo1", 20*time.Millisecond, func() { ch <- 1 })
	m.ResetOrCreate("foo2", 30*time.Millisecond, func() { ch <- 2 })
	m.StopAll()
	select {
	case x := <-ch:
		t.Fatalf("timer %d fired although it shouldn't", x)
	case <-time.After(100 * time.Millisecond):

	}

	if got, want := m.Len(), 0; got != want {
		t.Fatalf("got len %d want %d", got, want)
	}
}
