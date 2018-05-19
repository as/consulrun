package checks

import (
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	osexec "os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/armon/circbuf"
	"github.com/hashicorp/consul/agent/exec"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/lib"
	"github.com/hashicorp/consul/types"
	"github.com/hashicorp/go-cleanhttp"
)

const (
	MinInterval = time.Second

	BufSize = 4 * 1024 // 4KB

	UserAgent = "Consul Health Check"
)

type CheckNotifier interface {
	UpdateCheck(checkID types.CheckID, status, output string)
}

type CheckMonitor struct {
	Notify     CheckNotifier
	CheckID    types.CheckID
	Script     string
	ScriptArgs []string
	Interval   time.Duration
	Timeout    time.Duration
	Logger     *log.Logger

	stop     bool
	stopCh   chan struct{}
	stopLock sync.Mutex
}

func (c *CheckMonitor) Start() {
	c.stopLock.Lock()
	defer c.stopLock.Unlock()
	c.stop = false
	c.stopCh = make(chan struct{})
	go c.run()
}

func (c *CheckMonitor) Stop() {
	c.stopLock.Lock()
	defer c.stopLock.Unlock()
	if !c.stop {
		c.stop = true
		close(c.stopCh)
	}
}

func (c *CheckMonitor) run() {

	initialPauseTime := lib.RandomStagger(c.Interval)
	next := time.After(initialPauseTime)
	for {
		select {
		case <-next:
			c.check()
			next = time.After(c.Interval)
		case <-c.stopCh:
			return
		}
	}
}

func (c *CheckMonitor) check() {

	var cmd *osexec.Cmd
	var err error
	if len(c.ScriptArgs) > 0 {
		cmd, err = exec.Subprocess(c.ScriptArgs)
	} else {
		cmd, err = exec.Script(c.Script)
	}
	if err != nil {
		c.Logger.Printf("[ERR] agent: Check %q failed to setup: %s", c.CheckID, err)
		c.Notify.UpdateCheck(c.CheckID, api.HealthCritical, err.Error())
		return
	}

	output, _ := circbuf.NewBuffer(BufSize)
	cmd.Stdout = output
	cmd.Stderr = output
	exec.SetSysProcAttr(cmd)

	truncateAndLogOutput := func() string {
		outputStr := string(output.Bytes())
		if output.TotalWritten() > output.Size() {
			outputStr = fmt.Sprintf("Captured %d of %d bytes\n...\n%s",
				output.Size(), output.TotalWritten(), outputStr)
		}
		c.Logger.Printf("[TRACE] agent: Check %q output: %s", c.CheckID, outputStr)
		return outputStr
	}

	if err := cmd.Start(); err != nil {
		c.Logger.Printf("[ERR] agent: Check %q failed to invoke: %s", c.CheckID, err)
		c.Notify.UpdateCheck(c.CheckID, api.HealthCritical, err.Error())
		return
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	timeout := 30 * time.Second
	if c.Timeout > 0 {
		timeout = c.Timeout
	}
	select {
	case <-time.After(timeout):
		if err := exec.KillCommandSubtree(cmd); err != nil {
			c.Logger.Printf("[WARN] agent: Check %q failed to kill after timeout: %s", c.CheckID, err)
		}

		msg := fmt.Sprintf("Timed out (%s) running check", timeout.String())
		c.Logger.Printf("[WARN] agent: Check %q: %s", c.CheckID, msg)

		outputStr := truncateAndLogOutput()
		if len(outputStr) > 0 {
			msg += "\n\n" + outputStr
		}
		c.Notify.UpdateCheck(c.CheckID, api.HealthCritical, msg)

		<-waitCh
		return

	case err = <-waitCh:

	}

	outputStr := truncateAndLogOutput()
	if err == nil {
		c.Logger.Printf("[DEBUG] agent: Check %q is passing", c.CheckID)
		c.Notify.UpdateCheck(c.CheckID, api.HealthPassing, outputStr)
		return
	}

	exitErr, ok := err.(*osexec.ExitError)
	if ok {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			code := status.ExitStatus()
			if code == 1 {
				c.Logger.Printf("[WARN] agent: Check %q is now warning", c.CheckID)
				c.Notify.UpdateCheck(c.CheckID, api.HealthWarning, outputStr)
				return
			}
		}
	}

	c.Logger.Printf("[WARN] agent: Check %q is now critical", c.CheckID)
	c.Notify.UpdateCheck(c.CheckID, api.HealthCritical, outputStr)
}

type CheckTTL struct {
	Notify  CheckNotifier
	CheckID types.CheckID
	TTL     time.Duration
	Logger  *log.Logger

	timer *time.Timer

	lastOutput     string
	lastOutputLock sync.RWMutex

	stop     bool
	stopCh   chan struct{}
	stopLock sync.Mutex
}

func (c *CheckTTL) Start() {
	c.stopLock.Lock()
	defer c.stopLock.Unlock()
	c.stop = false
	c.stopCh = make(chan struct{})
	c.timer = time.NewTimer(c.TTL)
	go c.run()
}

func (c *CheckTTL) Stop() {
	c.stopLock.Lock()
	defer c.stopLock.Unlock()
	if !c.stop {
		c.timer.Stop()
		c.stop = true
		close(c.stopCh)
	}
}

func (c *CheckTTL) run() {
	for {
		select {
		case <-c.timer.C:
			c.Logger.Printf("[WARN] agent: Check %q missed TTL, is now critical",
				c.CheckID)
			c.Notify.UpdateCheck(c.CheckID, api.HealthCritical, c.getExpiredOutput())

		case <-c.stopCh:
			return
		}
	}
}

func (c *CheckTTL) getExpiredOutput() string {
	c.lastOutputLock.RLock()
	defer c.lastOutputLock.RUnlock()

	const prefix = "TTL expired"
	if c.lastOutput == "" {
		return prefix
	}

	return fmt.Sprintf("%s (last output before timeout follows): %s", prefix, c.lastOutput)
}

func (c *CheckTTL) SetStatus(status, output string) {
	c.Logger.Printf("[DEBUG] agent: Check %q status is now %s", c.CheckID, status)
	c.Notify.UpdateCheck(c.CheckID, status, output)

	c.lastOutputLock.Lock()
	c.lastOutput = output
	c.lastOutputLock.Unlock()

	c.timer.Reset(c.TTL)
}

type CheckHTTP struct {
	Notify          CheckNotifier
	CheckID         types.CheckID
	HTTP            string
	Header          map[string][]string
	Method          string
	Interval        time.Duration
	Timeout         time.Duration
	Logger          *log.Logger
	TLSClientConfig *tls.Config

	httpClient *http.Client
	stop       bool
	stopCh     chan struct{}
	stopLock   sync.Mutex
}

func (c *CheckHTTP) Start() {
	c.stopLock.Lock()
	defer c.stopLock.Unlock()

	if c.httpClient == nil {

		trans := cleanhttp.DefaultTransport()
		trans.DisableKeepAlives = true

		trans.TLSClientConfig = c.TLSClientConfig

		c.httpClient = &http.Client{
			Timeout:   10 * time.Second,
			Transport: trans,
		}

		if c.Timeout > 0 && c.Timeout < c.Interval {
			c.httpClient.Timeout = c.Timeout
		} else if c.Interval < 10*time.Second {
			c.httpClient.Timeout = c.Interval
		}
	}

	c.stop = false
	c.stopCh = make(chan struct{})
	go c.run()
}

func (c *CheckHTTP) Stop() {
	c.stopLock.Lock()
	defer c.stopLock.Unlock()
	if !c.stop {
		c.stop = true
		close(c.stopCh)
	}
}

func (c *CheckHTTP) run() {

	initialPauseTime := lib.RandomStagger(c.Interval)
	next := time.After(initialPauseTime)
	for {
		select {
		case <-next:
			c.check()
			next = time.After(c.Interval)
		case <-c.stopCh:
			return
		}
	}
}

func (c *CheckHTTP) check() {
	method := c.Method
	if method == "" {
		method = "GET"
	}

	req, err := http.NewRequest(method, c.HTTP, nil)
	if err != nil {
		c.Logger.Printf("[WARN] agent: Check %q HTTP request failed: %s", c.CheckID, err)
		c.Notify.UpdateCheck(c.CheckID, api.HealthCritical, err.Error())
		return
	}

	req.Header = http.Header(c.Header)

	if req.Header == nil {
		req.Header = make(http.Header)
	}

	if host := req.Header.Get("Host"); host != "" {
		req.Host = host
	}

	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", UserAgent)
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "text/plain, text/*, */*")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.Logger.Printf("[WARN] agent: Check %q HTTP request failed: %s", c.CheckID, err)
		c.Notify.UpdateCheck(c.CheckID, api.HealthCritical, err.Error())
		return
	}
	defer resp.Body.Close()

	output, _ := circbuf.NewBuffer(BufSize)
	if _, err := io.Copy(output, resp.Body); err != nil {
		c.Logger.Printf("[WARN] agent: Check %q error while reading body: %s", c.CheckID, err)
	}

	result := fmt.Sprintf("HTTP %s %s: %s Output: %s", method, c.HTTP, resp.Status, output.String())

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {

		c.Logger.Printf("[DEBUG] agent: Check %q is passing", c.CheckID)
		c.Notify.UpdateCheck(c.CheckID, api.HealthPassing, result)

	} else if resp.StatusCode == 429 {

		c.Logger.Printf("[WARN] agent: Check %q is now warning", c.CheckID)
		c.Notify.UpdateCheck(c.CheckID, api.HealthWarning, result)

	} else {

		c.Logger.Printf("[WARN] agent: Check %q is now critical", c.CheckID)
		c.Notify.UpdateCheck(c.CheckID, api.HealthCritical, result)
	}
}

type CheckTCP struct {
	Notify   CheckNotifier
	CheckID  types.CheckID
	TCP      string
	Interval time.Duration
	Timeout  time.Duration
	Logger   *log.Logger

	dialer   *net.Dialer
	stop     bool
	stopCh   chan struct{}
	stopLock sync.Mutex
}

func (c *CheckTCP) Start() {
	c.stopLock.Lock()
	defer c.stopLock.Unlock()

	if c.dialer == nil {

		c.dialer = &net.Dialer{DualStack: true}

		if c.Timeout > 0 && c.Timeout < c.Interval {
			c.dialer.Timeout = c.Timeout
		} else if c.Interval < 10*time.Second {
			c.dialer.Timeout = c.Interval
		}
	}

	c.stop = false
	c.stopCh = make(chan struct{})
	go c.run()
}

func (c *CheckTCP) Stop() {
	c.stopLock.Lock()
	defer c.stopLock.Unlock()
	if !c.stop {
		c.stop = true
		close(c.stopCh)
	}
}

func (c *CheckTCP) run() {

	initialPauseTime := lib.RandomStagger(c.Interval)
	next := time.After(initialPauseTime)
	for {
		select {
		case <-next:
			c.check()
			next = time.After(c.Interval)
		case <-c.stopCh:
			return
		}
	}
}

func (c *CheckTCP) check() {
	conn, err := c.dialer.Dial(`tcp`, c.TCP)
	if err != nil {
		c.Logger.Printf("[WARN] agent: Check %q socket connection failed: %s", c.CheckID, err)
		c.Notify.UpdateCheck(c.CheckID, api.HealthCritical, err.Error())
		return
	}
	conn.Close()
	c.Logger.Printf("[DEBUG] agent: Check %q is passing", c.CheckID)
	c.Notify.UpdateCheck(c.CheckID, api.HealthPassing, fmt.Sprintf("TCP connect %s: Success", c.TCP))
}

type CheckDocker struct {
	Notify            CheckNotifier
	CheckID           types.CheckID
	Script            string
	ScriptArgs        []string
	DockerContainerID string
	Shell             string
	Interval          time.Duration
	Logger            *log.Logger
	Client            *DockerClient

	stop chan struct{}
}

func (c *CheckDocker) Start() {
	if c.stop != nil {
		panic("Docker check already started")
	}

	if c.Logger == nil {
		c.Logger = log.New(ioutil.Discard, "", 0)
	}

	if c.Shell == "" {
		c.Shell = os.Getenv("SHELL")
		if c.Shell == "" {
			c.Shell = "/bin/sh"
		}
	}
	c.stop = make(chan struct{})
	go c.run()
}

func (c *CheckDocker) Stop() {
	if c.stop == nil {
		panic("Stop called before start")
	}
	close(c.stop)
}

func (c *CheckDocker) run() {
	defer c.Client.Close()
	firstWait := lib.RandomStagger(c.Interval)
	next := time.After(firstWait)
	for {
		select {
		case <-next:
			c.check()
			next = time.After(c.Interval)
		case <-c.stop:
			return
		}
	}
}

func (c *CheckDocker) check() {
	var out string
	status, b, err := c.doCheck()
	if err != nil {
		c.Logger.Printf("[DEBUG] agent: Check %q: %s", c.CheckID, err)
		out = err.Error()
	} else {

		out = string(b.Bytes())
		if int(b.TotalWritten()) > len(out) {
			out = fmt.Sprintf("Captured %d of %d bytes\n...\n%s", len(out), b.TotalWritten(), out)
		}
		c.Logger.Printf("[TRACE] agent: Check %q output: %s", c.CheckID, out)
	}

	if status == api.HealthCritical {
		c.Logger.Printf("[WARN] agent: Check %q is now critical", c.CheckID)
	}

	c.Notify.UpdateCheck(c.CheckID, status, out)
}

func (c *CheckDocker) doCheck() (string, *circbuf.Buffer, error) {
	var cmd []string
	if len(c.ScriptArgs) > 0 {
		cmd = c.ScriptArgs
	} else {
		cmd = []string{c.Shell, "-c", c.Script}
	}

	execID, err := c.Client.CreateExec(c.DockerContainerID, cmd)
	if err != nil {
		return api.HealthCritical, nil, err
	}

	buf, err := c.Client.StartExec(c.DockerContainerID, execID)
	if err != nil {
		return api.HealthCritical, nil, err
	}

	exitCode, err := c.Client.InspectExec(c.DockerContainerID, execID)
	if err != nil {
		return api.HealthCritical, nil, err
	}

	switch exitCode {
	case 0:
		return api.HealthPassing, buf, nil
	case 1:
		c.Logger.Printf("[DEBUG] agent: Check %q failed with exit code: %d", c.CheckID, exitCode)
		return api.HealthWarning, buf, nil
	default:
		c.Logger.Printf("[DEBUG] agent: Check %q failed with exit code: %d", c.CheckID, exitCode)
		return api.HealthCritical, buf, nil
	}
}

type CheckGRPC struct {
	Notify          CheckNotifier
	CheckID         types.CheckID
	GRPC            string
	Interval        time.Duration
	Timeout         time.Duration
	TLSClientConfig *tls.Config
	Logger          *log.Logger

	probe    *GrpcHealthProbe
	stop     bool
	stopCh   chan struct{}
	stopLock sync.Mutex
}

func (c *CheckGRPC) Start() {
	c.stopLock.Lock()
	defer c.stopLock.Unlock()
	timeout := 10 * time.Second
	if c.Timeout > 0 {
		timeout = c.Timeout
	}
	c.probe = NewGrpcHealthProbe(c.GRPC, timeout, c.TLSClientConfig)
	c.stop = false
	c.stopCh = make(chan struct{})
	go c.run()
}

func (c *CheckGRPC) run() {

	initialPauseTime := lib.RandomStagger(c.Interval)
	next := time.After(initialPauseTime)
	for {
		select {
		case <-next:
			c.check()
			next = time.After(c.Interval)
		case <-c.stopCh:
			return
		}
	}
}

func (c *CheckGRPC) check() {
	err := c.probe.Check()
	if err != nil {
		c.Logger.Printf("[DEBUG] agent: Check %q failed: %s", c.CheckID, err.Error())
		c.Notify.UpdateCheck(c.CheckID, api.HealthCritical, err.Error())
	} else {
		c.Logger.Printf("[DEBUG] agent: Check %q is passing", c.CheckID)
		c.Notify.UpdateCheck(c.CheckID, api.HealthPassing, fmt.Sprintf("gRPC check %s: success", c.GRPC))
	}
}

func (c *CheckGRPC) Stop() {
	c.stopLock.Lock()
	defer c.stopLock.Unlock()
	if !c.stop {
		c.stop = true
		close(c.stopCh)
	}
}
