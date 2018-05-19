package logger

import (
	"testing"
)

type MockLogHandler struct {
	logs []string
}

func (m *MockLogHandler) HandleLog(l string) {
	m.logs = append(m.logs, l)
}

func TestLogWriter(t *testing.T) {
	h := &MockLogHandler{}
	w := NewLogWriter(4)

	w.Write([]byte("one")) // Gets dropped!
	w.Write([]byte("two"))
	w.Write([]byte("three"))
	w.Write([]byte("four"))
	w.Write([]byte("five"))

	w.RegisterHandler(h)

	w.Write([]byte("six"))
	w.Write([]byte("seven"))

	w.DeregisterHandler(h)

	w.Write([]byte("eight"))
	w.Write([]byte("nine"))

	out := []string{
		"two",
		"three",
		"four",
		"five",
		"six",
		"seven",
	}
	for idx := range out {
		if out[idx] != h.logs[idx] {
			t.Fatalf("mismatch %v", h.logs)
		}
	}
}
