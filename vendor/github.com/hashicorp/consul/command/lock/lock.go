package lock

import (
	"flag"
	"fmt"
	"os"
	osexec "os/exec"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/consul/agent"
	"github.com/hashicorp/consul/agent/exec"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/command/flags"
	"github.com/mitchellh/cli"
)

const (
	lockKillGracePeriod = 5 * time.Second

	defaultMonitorRetry = 3

	defaultMonitorRetryTime = 1 * time.Second
)

type cmd struct {
	UI    cli.Ui
	flags *flag.FlagSet
	http  *flags.HTTPFlags
	help  string

	ShutdownCh <-chan struct{}

	child     *os.Process
	childLock sync.Mutex
	verbose   bool

	limit              int
	monitorRetry       int
	name               string
	passStdin          bool
	propagateChildCode bool
	shell              bool
	timeout            time.Duration
}

func New(ui cli.Ui) *cmd {
	c := &cmd{UI: ui}
	c.init()
	return c
}

func (c *cmd) init() {
	c.flags = flag.NewFlagSet("", flag.ContinueOnError)
	c.flags.BoolVar(&c.propagateChildCode, "child-exit-code", false,
		"Exit 2 if the child process exited with an error if this is true, "+
			"otherwise this doesn't propagate an error from the child. The "+
			"default value is false.")
	c.flags.IntVar(&c.limit, "n", 1,
		"Optional limit on the number of concurrent lock holders. The underlying "+
			"implementation switches from a lock to a semaphore when the value is "+
			"greater than 1. The default value is 1.")
	c.flags.IntVar(&c.monitorRetry, "monitor-retry", defaultMonitorRetry,
		"Number of times to retry if Consul returns a 500 error while monitoring "+
			"the lock. This allows riding out brief periods of unavailability "+
			"without causing leader elections, but increases the amount of time "+
			"required to detect a lost lock in some cases. The default value is 3, "+
			"with a 1s wait between retries. Set this value to 0 to disable retires.")
	c.flags.StringVar(&c.name, "name", "",
		"Optional name to associate with the lock session. It not provided, one "+
			"is generated based on the provided child command.")
	c.flags.BoolVar(&c.passStdin, "pass-stdin", false,
		"Pass stdin to the child process.")
	c.flags.BoolVar(&c.shell, "shell", true,
		"Use a shell to run the command (can set a custom shell via the SHELL "+
			"environment variable).")
	c.flags.DurationVar(&c.timeout, "timeout", 0,
		"Maximum amount of time to wait to acquire the lock, specified as a "+
			"duration like \"1s\" or \"3h\". The default value is 0.")
	c.flags.BoolVar(&c.verbose, "verbose", false,
		"Enable verbose (debugging) output.")

	c.flags.DurationVar(&c.timeout, "try", 0,
		"DEPRECATED. Use -timeout instead.")

	c.http = &flags.HTTPFlags{}
	flags.Merge(c.flags, c.http.ClientFlags())
	flags.Merge(c.flags, c.http.ServerFlags())
	c.help = flags.Usage(help, c.flags)
}

func (c *cmd) Run(args []string) int {
	var lu *LockUnlock
	return c.run(args, &lu)
}

func (c *cmd) run(args []string, lu **LockUnlock) int {
	if err := c.flags.Parse(args); err != nil {
		return 1
	}

	if c.limit <= 0 {
		c.UI.Error(fmt.Sprintf("Lock holder limit must be positive"))
		return 1
	}

	extra := c.flags.Args()
	if len(extra) < 2 {
		c.UI.Error("Key prefix and child command must be specified")
		return 1
	}
	prefix := extra[0]
	prefix = strings.TrimPrefix(prefix, "/")

	if c.timeout < 0 {
		c.UI.Error("Timeout must be positive")
		return 1
	}

	if c.name == "" {
		c.name = fmt.Sprintf("Consul lock for '%s' at '%s'", strings.Join(extra[1:], " "), prefix)
	}

	oneshot := c.timeout > 0

	if c.monitorRetry < 0 {
		c.UI.Error("Number for 'monitor-retry' must be >= 0")
		return 1
	}

	client, err := c.http.APIClient()
	if err != nil {
		c.UI.Error(fmt.Sprintf("Error connecting to Consul agent: %s", err))
		return 1
	}
	_, err = client.Agent().NodeName()
	if err != nil {
		c.UI.Error(fmt.Sprintf("Error querying Consul agent: %s", err))
		return 1
	}

	if c.limit == 1 {
		*lu, err = c.setupLock(client, prefix, c.name, oneshot, c.timeout, c.monitorRetry)
	} else {
		*lu, err = c.setupSemaphore(client, c.limit, prefix, c.name, oneshot, c.timeout, c.monitorRetry)
	}
	if err != nil {
		c.UI.Error(fmt.Sprintf("Lock setup failed: %s", err))
		return 1
	}

	if c.verbose {
		c.UI.Info("Attempting lock acquisition")
	}
	lockCh, err := (*lu).lockFn(c.ShutdownCh)
	if lockCh == nil {
		if err == nil {
			c.UI.Error("Shutdown triggered or timeout during lock acquisition")
		} else {
			c.UI.Error(fmt.Sprintf("Lock acquisition failed: %s", err))
		}
		return 1
	}

	var childCode int
	var childErr chan error
	select {
	case <-c.ShutdownCh:
		c.UI.Error("Shutdown triggered during lock acquisition")
		goto RELEASE
	default:
	}

	childErr = make(chan error, 1)
	go func() {
		childErr <- c.startChild(c.flags.Args()[1:], c.passStdin, c.shell)
	}()

	select {
	case <-c.ShutdownCh:
		if c.verbose {
			c.UI.Info("Shutdown triggered, killing child")
		}
	case <-lockCh:
		if c.verbose {
			c.UI.Info("Lock lost, killing child")
		}
	case err := <-childErr:
		if err != nil {
			childCode = 2
		}
		if c.verbose {
			c.UI.Info("Child terminated, releasing lock")
		}
		goto RELEASE
	}

	c.childLock.Lock()

	if err := c.killChild(childErr); err != nil {
		c.UI.Error(fmt.Sprintf("%s", err))
	}

RELEASE:

	if err := (*lu).unlockFn(); err != nil {
		c.UI.Error(fmt.Sprintf("Lock release failed: %s", err))
		return 1
	}

	if err := (*lu).cleanupFn(); err != nil {
		if err != (*lu).inUseErr {
			c.UI.Error(fmt.Sprintf("Lock cleanup failed: %s", err))
			return 1
		} else if c.verbose {
			c.UI.Info("Cleanup aborted, lock in use")
		}
	} else if c.verbose {
		c.UI.Info("Cleanup succeeded")
	}

	if c.propagateChildCode {
		return childCode
	}

	return 0
}

func (c *cmd) setupLock(client *api.Client, prefix, name string,
	oneshot bool, wait time.Duration, retry int) (*LockUnlock, error) {

	key := path.Join(prefix, api.DefaultSemaphoreKey)
	if c.verbose {
		c.UI.Info(fmt.Sprintf("Setting up lock at path: %s", key))
	}
	opts := api.LockOptions{
		Key:              key,
		SessionName:      name,
		MonitorRetries:   retry,
		MonitorRetryTime: defaultMonitorRetryTime,
	}
	if oneshot {
		opts.LockTryOnce = true
		opts.LockWaitTime = wait
	}
	l, err := client.LockOpts(&opts)
	if err != nil {
		return nil, err
	}
	lu := &LockUnlock{
		lockFn:    l.Lock,
		unlockFn:  l.Unlock,
		cleanupFn: l.Destroy,
		inUseErr:  api.ErrLockInUse,
		rawOpts:   &opts,
	}
	return lu, nil
}

func (c *cmd) setupSemaphore(client *api.Client, limit int, prefix, name string,
	oneshot bool, wait time.Duration, retry int) (*LockUnlock, error) {
	if c.verbose {
		c.UI.Info(fmt.Sprintf("Setting up semaphore (limit %d) at prefix: %s", limit, prefix))
	}
	opts := api.SemaphoreOptions{
		Prefix:           prefix,
		Limit:            limit,
		SessionName:      name,
		MonitorRetries:   retry,
		MonitorRetryTime: defaultMonitorRetryTime,
	}
	if oneshot {
		opts.SemaphoreTryOnce = true
		opts.SemaphoreWaitTime = wait
	}
	s, err := client.SemaphoreOpts(&opts)
	if err != nil {
		return nil, err
	}
	lu := &LockUnlock{
		lockFn:    s.Acquire,
		unlockFn:  s.Release,
		cleanupFn: s.Destroy,
		inUseErr:  api.ErrSemaphoreInUse,
		rawOpts:   &opts,
	}
	return lu, nil
}

func (c *cmd) startChild(args []string, passStdin, shell bool) error {
	if c.verbose {
		c.UI.Info("Starting handler")
	}

	var cmd *osexec.Cmd
	var err error
	if !shell {
		cmd, err = exec.Subprocess(args)
	} else {
		cmd, err = exec.Script(strings.Join(args, " "))
	}
	if err != nil {
		c.UI.Error(fmt.Sprintf("Error executing handler: %s", err))
		return err
	}

	cmd.Env = append(os.Environ(),
		"CONSUL_LOCK_HELD=true",
	)
	if passStdin {
		if c.verbose {
			c.UI.Info("Stdin passed to handler process")
		}
		cmd.Stdin = os.Stdin
	} else {
		cmd.Stdin = nil
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	c.childLock.Lock()
	if err := cmd.Start(); err != nil {
		c.UI.Error(fmt.Sprintf("Error starting handler: %s", err))
		c.childLock.Unlock()
		return err
	}

	doneCh := make(chan struct{})
	defer close(doneCh)
	logFn := func(err error) {
		c.UI.Error(fmt.Sprintf("Warning, could not forward signal: %s", err))
	}
	agent.ForwardSignals(cmd, logFn, doneCh)

	c.child = cmd.Process
	c.childLock.Unlock()

	if err := cmd.Wait(); err != nil {
		c.UI.Error(fmt.Sprintf("Error running handler: %s", err))
		return err
	}
	return nil
}

func (c *cmd) killChild(childErr chan error) error {

	child := c.child

	if child == nil {
		if c.verbose {
			c.UI.Info("No child process to kill")
		}
		return nil
	}

	if c.verbose {
		c.UI.Info(fmt.Sprintf("Terminating child pid %d", child.Pid))
	}
	if err := signalPid(child.Pid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("Failed to terminate %d: %v", child.Pid, err)
	}

	select {
	case <-childErr:
		if c.verbose {
			c.UI.Info("Child terminated")
		}
		return nil
	case <-time.After(lockKillGracePeriod):
		if c.verbose {
			c.UI.Info(fmt.Sprintf("Child did not exit after grace period of %v",
				lockKillGracePeriod))
		}
	}

	if c.verbose {
		c.UI.Info(fmt.Sprintf("Killing child pid %d", child.Pid))
	}
	if err := signalPid(child.Pid, syscall.SIGKILL); err != nil {
		return fmt.Errorf("Failed to kill %d: %v", child.Pid, err)
	}
	return nil
}

func (c *cmd) Synopsis() string {
	return synopsis
}

func (c *cmd) Help() string {
	return c.help
}

type LockUnlock struct {
	lockFn    func(<-chan struct{}) (<-chan struct{}, error)
	unlockFn  func() error
	cleanupFn func() error
	inUseErr  error
	rawOpts   interface{}
}

const synopsis = "Execute a command holding a lock"
const help = `
Usage: consul lock [options] prefix child...

  Acquires a lock or semaphore at a given path, and invokes a child process
  when successful. The child process can assume the lock is held while it
  executes. If the lock is lost or communication is disrupted the child
  process will be sent a SIGTERM signal and given time to gracefully exit.
  After the grace period expires the process will be hard terminated.

  For Consul agents on Windows, the child process is always hard terminated
  with a SIGKILL, since Windows has no POSIX compatible notion for SIGTERM.

  When -n=1, only a single lock holder or leader exists providing mutual
  exclusion. Setting a higher value switches to a semaphore allowing multiple
  holders to coordinate.

  The prefix provided must have write privileges.
`
