package monitor

import (
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/agent"
	"github.com/hashicorp/consul/logger"
	"github.com/mitchellh/cli"
)

func TestMonitorCommand_exitsOnSignalBeforeLinesArrive(t *testing.T) {
	t.Parallel()
	logWriter := logger.NewLogWriter(512)
	a := &agent.TestAgent{
		Name:      t.Name(),
		LogWriter: logWriter,
		LogOutput: io.MultiWriter(os.Stderr, logWriter),
	}
	a.Start()
	defer a.Shutdown()

	shutdownCh := make(chan struct{})

	ui := cli.NewMockUi()
	c := New(ui, shutdownCh)

	args := []string{"-http-addr=" + a.HTTPAddr(), "-log-level=ERR"}

	exitCode := make(chan int, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done() // Signal that this goroutine is at least running now
		exitCode <- c.Run(args)
	}()

	wg.Wait()

	go func() {
		time.Sleep(5 * time.Millisecond)
		shutdownCh <- struct{}{}
	}()

	select {
	case ret := <-exitCode:
		if ret != 0 {
			t.Fatal("command returned with non-zero code")
		}

	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for exit")
	}
}
