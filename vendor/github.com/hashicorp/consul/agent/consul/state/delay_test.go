package state

import (
	"testing"
	"time"
)

func TestDelay(t *testing.T) {
	d := NewDelay()

	if exp := d.GetExpiration("nope"); !exp.Before(time.Now()) {
		t.Fatalf("bad: %v", exp)
	}

	now := time.Now()
	delay := 250 * time.Millisecond
	d.SetExpiration("bye", now, delay)
	if exp := d.GetExpiration("bye"); !exp.After(now) {
		t.Fatalf("bad: %v", exp)
	}

	time.Sleep(2 * delay)
	if exp := d.GetExpiration("bye"); !exp.Before(now) {
		t.Fatalf("bad: %v", exp)
	}
}
