package exec

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/command/flags"
	"github.com/mitchellh/cli"
)

func New(ui cli.Ui, shutdownCh <-chan struct{}) *cmd {
	c := &cmd{UI: ui, shutdownCh: shutdownCh}
	c.init()
	return c
}

type cmd struct {
	UI    cli.Ui
	flags *flag.FlagSet
	http  *flags.HTTPFlags
	help  string

	shutdownCh <-chan struct{}
	conf       rExecConf
	apiclient  *api.Client
	sessionID  string
	stopCh     chan struct{}
}

func (c *cmd) init() {
	c.flags = flag.NewFlagSet("", flag.ContinueOnError)
	c.flags.StringVar(&c.conf.node, "node", "",
		"Regular expression to filter on node names.")
	c.flags.StringVar(&c.conf.service, "service", "",
		"Regular expression to filter on service instances.")
	c.flags.StringVar(&c.conf.tag, "tag", "",
		"Regular expression to filter on service tags. Must be used with -service.")
	c.flags.StringVar(&c.conf.prefix, "prefix", rExecPrefix,
		"Prefix in the KV store to use for request data.")
	c.flags.BoolVar(&c.conf.shell, "shell", true,
		"Use a shell to run the command.")
	c.flags.DurationVar(&c.conf.wait, "wait", rExecQuietWait,
		"Period to wait with no responses before terminating execution.")
	c.flags.DurationVar(&c.conf.replWait, "wait-repl", rExecReplicationWait,
		"Period to wait for replication before firing event. This is an optimization to allow stale reads to be performed.")
	c.flags.BoolVar(&c.conf.verbose, "verbose", false,
		"Enables verbose output.")

	c.http = &flags.HTTPFlags{}
	flags.Merge(c.flags, c.http.ClientFlags())
	flags.Merge(c.flags, c.http.ServerFlags())
	c.help = flags.Usage(help, c.flags)
}

func (c *cmd) Run(args []string) int {
	if err := c.flags.Parse(args); err != nil {
		return 1
	}

	c.conf.cmd = strings.Join(c.flags.Args(), " ")

	if c.conf.cmd == "-" {
		if !c.conf.shell {
			c.UI.Error("Cannot configure -shell=false when reading from stdin")
			return 1
		}

		c.conf.cmd = ""
		var buf bytes.Buffer
		_, err := io.Copy(&buf, os.Stdin)
		if err != nil {
			c.UI.Error(fmt.Sprintf("Failed to read stdin: %v", err))
			c.UI.Error("")
			c.UI.Error(c.Help())
			return 1
		}
		c.conf.script = buf.Bytes()
	} else if !c.conf.shell {
		c.conf.cmd = ""
		c.conf.args = c.flags.Args()
	}

	if c.conf.cmd == "" && len(c.conf.script) == 0 && len(c.conf.args) == 0 {
		c.UI.Error("Must specify a command to execute")
		c.UI.Error("")
		c.UI.Error(c.Help())
		return 1
	}

	if err := c.conf.validate(); err != nil {
		c.UI.Error(err.Error())
		return 1
	}

	client, err := c.http.APIClient()
	if err != nil {
		c.UI.Error(fmt.Sprintf("Error connecting to Consul agent: %s", err))
		return 1
	}
	info, err := client.Agent().Self()
	if err != nil {
		c.UI.Error(fmt.Sprintf("Error querying Consul agent: %s", err))
		return 1
	}
	c.apiclient = client

	if c.http.Datacenter() != "" && c.http.Datacenter() != info["Config"]["Datacenter"] {
		if c.conf.verbose {
			c.UI.Info("Remote exec in foreign datacenter, using Session TTL")
		}
		c.conf.foreignDC = true
		c.conf.localDC = info["Config"]["Datacenter"].(string)
		c.conf.localNode = info["Config"]["NodeName"].(string)
	}

	spec, err := c.makeRExecSpec()
	if err != nil {
		c.UI.Error(fmt.Sprintf("Failed to create job spec: %s", err))
		return 1
	}

	c.sessionID, err = c.createSession()
	if err != nil {
		c.UI.Error(fmt.Sprintf("Failed to create session: %s", err))
		return 1
	}
	defer c.destroySession()
	if c.conf.verbose {
		c.UI.Info(fmt.Sprintf("Created remote execution session: %s", c.sessionID))
	}

	if err := c.uploadPayload(spec); err != nil {
		c.UI.Error(fmt.Sprintf("Failed to create job file: %s", err))
		return 1
	}
	defer c.destroyData()
	if c.conf.verbose {
		c.UI.Info(fmt.Sprintf("Uploaded remote execution spec"))
	}

	select {
	case <-time.After(c.conf.replWait):
	case <-c.shutdownCh:
		return 1
	}

	id, err := c.fireEvent()
	if err != nil {
		c.UI.Error(fmt.Sprintf("Failed to fire event: %s", err))
		return 1
	}
	if c.conf.verbose {
		c.UI.Info(fmt.Sprintf("Fired remote execution event: %s", id))
	}

	return c.waitForJob()
}

func (c *cmd) Synopsis() string {
	return synopsis
}

func (c *cmd) Help() string {
	return c.help
}

const synopsis = "Executes a command on Consul nodes"
const help = `
Usage: consul exec [options] [-|command...]

  Evaluates a command on remote Consul nodes. The nodes responding can
  be filtered using regular expressions on node name, service, and tag
  definitions. If a command is '-', stdin will be read until EOF
  and used as a script input.
`

func (c *cmd) waitForJob() int {

	defer c.destroySession()
	start := time.Now()
	ackCh := make(chan rExecAck, 128)
	heartCh := make(chan rExecHeart, 128)
	outputCh := make(chan rExecOutput, 128)
	exitCh := make(chan rExecExit, 128)
	doneCh := make(chan struct{})
	errCh := make(chan struct{}, 1)
	defer close(doneCh)
	go c.streamResults(doneCh, ackCh, heartCh, outputCh, exitCh, errCh)
	target := &TargetedUI{UI: c.UI}

	var ackCount, exitCount, badExit int
OUTER:
	for {

		waitIntv := c.conf.wait
		if ackCount > exitCount {
			waitIntv *= 2
		}

		select {
		case e := <-ackCh:
			ackCount++
			if c.conf.verbose {
				target.Target = e.Node
				target.Info("acknowledged")
			}

		case h := <-heartCh:
			if c.conf.verbose {
				target.Target = h.Node
				target.Info("heartbeat received")
			}

		case e := <-outputCh:
			target.Target = e.Node
			target.Output(string(e.Output))

		case e := <-exitCh:
			exitCount++
			target.Target = e.Node
			target.Info(fmt.Sprintf("finished with exit code %d", e.Code))
			if e.Code != 0 {
				badExit++
			}

		case <-time.After(waitIntv):
			c.UI.Info(fmt.Sprintf("%d / %d node(s) completed / acknowledged", exitCount, ackCount))
			if c.conf.verbose {
				c.UI.Info(fmt.Sprintf("Completed in %0.2f seconds",
					float64(time.Since(start))/float64(time.Second)))
			}
			if exitCount < ackCount {
				badExit++
			}
			break OUTER

		case <-errCh:
			return 1

		case <-c.shutdownCh:
			return 1
		}
	}

	if badExit > 0 {
		return 2
	}
	return 0
}

func (c *cmd) streamResults(doneCh chan struct{}, ackCh chan rExecAck, heartCh chan rExecHeart,
	outputCh chan rExecOutput, exitCh chan rExecExit, errCh chan struct{}) {
	kv := c.apiclient.KV()
	opts := api.QueryOptions{WaitTime: c.conf.wait}
	dir := path.Join(c.conf.prefix, c.sessionID) + "/"
	seen := make(map[string]struct{})

	for {

		select {
		case <-doneCh:
			return
		default:
		}

		keys, qm, err := kv.Keys(dir, "", &opts)
		if err != nil {
			c.UI.Error(fmt.Sprintf("Failed to read results: %s", err))
			goto ERR_EXIT
		}

		if qm.LastIndex == opts.WaitIndex {
			continue
		}
		opts.WaitIndex = qm.LastIndex

		for _, key := range keys {

			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}

			full := key
			key = strings.TrimPrefix(key, dir)

			switch {
			case key == rExecFileName:
				continue
			case strings.HasSuffix(key, rExecAckSuffix):
				ackCh <- rExecAck{Node: strings.TrimSuffix(key, rExecAckSuffix)}

			case strings.HasSuffix(key, rExecExitSuffix):
				pair, _, err := kv.Get(full, nil)
				if err != nil || pair == nil {
					c.UI.Error(fmt.Sprintf("Failed to read key '%s': %v", full, err))
					continue
				}
				code, err := strconv.ParseInt(string(pair.Value), 10, 32)
				if err != nil {
					c.UI.Error(fmt.Sprintf("Failed to parse exit code '%s': %v", pair.Value, err))
					continue
				}
				exitCh <- rExecExit{
					Node: strings.TrimSuffix(key, rExecExitSuffix),
					Code: int(code),
				}

			case strings.LastIndex(key, rExecOutputDivider) != -1:
				pair, _, err := kv.Get(full, nil)
				if err != nil || pair == nil {
					c.UI.Error(fmt.Sprintf("Failed to read key '%s': %v", full, err))
					continue
				}
				idx := strings.LastIndex(key, rExecOutputDivider)
				node := key[:idx]
				if len(pair.Value) == 0 {
					heartCh <- rExecHeart{Node: node}
				} else {
					outputCh <- rExecOutput{Node: node, Output: pair.Value}
				}

			default:
				c.UI.Error(fmt.Sprintf("Unknown key '%s', ignoring.", key))
			}
		}
	}

ERR_EXIT:
	select {
	case errCh <- struct{}{}:
	default:
	}
}

func (conf *rExecConf) validate() error {

	if conf.node != "" {
		if _, err := regexp.Compile(conf.node); err != nil {
			return fmt.Errorf("Failed to compile node filter regexp: %v", err)
		}
	}
	if conf.service != "" {
		if _, err := regexp.Compile(conf.service); err != nil {
			return fmt.Errorf("Failed to compile service filter regexp: %v", err)
		}
	}
	if conf.tag != "" {
		if _, err := regexp.Compile(conf.tag); err != nil {
			return fmt.Errorf("Failed to compile tag filter regexp: %v", err)
		}
	}
	if conf.tag != "" && conf.service == "" {
		return fmt.Errorf("Cannot provide tag filter without service filter.")
	}
	return nil
}

func (c *cmd) createSession() (string, error) {
	var id string
	var err error
	if c.conf.foreignDC {
		id, err = c.createSessionForeign()
	} else {
		id, err = c.createSessionLocal()
	}
	if err == nil {
		c.stopCh = make(chan struct{})
		go c.renewSession(id, c.stopCh)
	}
	return id, err
}

func (c *cmd) createSessionLocal() (string, error) {
	session := c.apiclient.Session()
	se := api.SessionEntry{
		Name:     "Remote Exec",
		Behavior: api.SessionBehaviorDelete,
		TTL:      rExecTTL,
	}
	id, _, err := session.Create(&se, nil)
	return id, err
}

func (c *cmd) createSessionForeign() (string, error) {

	health := c.apiclient.Health()
	services, _, err := health.Service("consul", "", true, nil)
	if err != nil {
		return "", fmt.Errorf("Failed to find Consul server in remote datacenter: %v", err)
	}
	if len(services) == 0 {
		return "", fmt.Errorf("Failed to find Consul server in remote datacenter")
	}
	node := services[0].Node.Node
	if c.conf.verbose {
		c.UI.Info(fmt.Sprintf("Binding session to remote node %s@%s", node, c.http.Datacenter()))
	}

	session := c.apiclient.Session()
	se := api.SessionEntry{
		Name:     fmt.Sprintf("Remote Exec via %s@%s", c.conf.localNode, c.conf.localDC),
		Node:     node,
		Checks:   []string{},
		Behavior: api.SessionBehaviorDelete,
		TTL:      rExecTTL,
	}
	id, _, err := session.CreateNoChecks(&se, nil)
	return id, err
}

func (c *cmd) renewSession(id string, stopCh chan struct{}) {
	session := c.apiclient.Session()
	for {
		select {
		case <-time.After(rExecRenewInterval):
			_, _, err := session.Renew(id, nil)
			if err != nil {
				c.UI.Error(fmt.Sprintf("Session renew failed: %v", err))
				return
			}
		case <-stopCh:
			return
		}
	}
}

func (c *cmd) destroySession() error {

	if c.stopCh != nil {
		close(c.stopCh)
		c.stopCh = nil
	}

	session := c.apiclient.Session()
	_, err := session.Destroy(c.sessionID, nil)
	return err
}

func (c *cmd) makeRExecSpec() ([]byte, error) {
	spec := &rExecSpec{
		Command: c.conf.cmd,
		Args:    c.conf.args,
		Script:  c.conf.script,
		Wait:    c.conf.wait,
	}
	return json.Marshal(spec)
}

func (c *cmd) uploadPayload(payload []byte) error {
	kv := c.apiclient.KV()
	pair := api.KVPair{
		Key:     path.Join(c.conf.prefix, c.sessionID, rExecFileName),
		Value:   payload,
		Session: c.sessionID,
	}
	ok, _, err := kv.Acquire(&pair, nil)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("failed to acquire key %s", pair.Key)
	}
	return nil
}

func (c *cmd) destroyData() error {
	kv := c.apiclient.KV()
	dir := path.Join(c.conf.prefix, c.sessionID)
	_, err := kv.DeleteTree(dir, nil)
	return err
}

func (c *cmd) fireEvent() (string, error) {

	msg := &rExecEvent{
		Prefix:  c.conf.prefix,
		Session: c.sessionID,
	}
	buf, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}

	event := c.apiclient.Event()
	params := &api.UserEvent{
		Name:          "_rexec",
		Payload:       buf,
		NodeFilter:    c.conf.node,
		ServiceFilter: c.conf.service,
		TagFilter:     c.conf.tag,
	}

	id, _, err := event.Fire(params, nil)
	return id, err
}

const (
	rExecPrefix = "_rexec"

	rExecFileName = "job"

	rExecAckSuffix = "/ack"

	rExecExitSuffix = "/exit"

	rExecOutputDivider = "/out/"

	rExecReplicationWait = 200 * time.Millisecond

	rExecQuietWait = 2 * time.Second

	rExecTTL = "15s"

	rExecRenewInterval = 5 * time.Second
)

type rExecConf struct {
	prefix string
	shell  bool

	foreignDC bool
	localDC   string
	localNode string

	node    string
	service string
	tag     string

	wait     time.Duration
	replWait time.Duration

	cmd    string
	args   []string
	script []byte

	verbose bool
}

type rExecEvent struct {
	Prefix  string
	Session string
}

type rExecSpec struct {
	Command string `json:",omitempty"`

	Args []string `json:",omitempty"`

	Script []byte `json:",omitempty"`

	Wait time.Duration
}

type rExecAck struct {
	Node string
}

type rExecHeart struct {
	Node string
}

type rExecOutput struct {
	Node   string
	Output []byte
}

type rExecExit struct {
	Node string
	Code int
}

type TargetedUI struct {
	Target string
	UI     cli.Ui
}

func (u *TargetedUI) Ask(query string) (string, error) {
	return u.UI.Ask(u.prefixLines(true, query))
}

func (u *TargetedUI) Info(message string) {
	u.UI.Info(u.prefixLines(true, message))
}

func (u *TargetedUI) Output(message string) {
	u.UI.Output(u.prefixLines(false, message))
}

func (u *TargetedUI) Error(message string) {
	u.UI.Error(u.prefixLines(true, message))
}

func (u *TargetedUI) prefixLines(arrow bool, message string) string {
	arrowText := "==>"
	if !arrow {
		arrowText = strings.Repeat(" ", len(arrowText))
	}

	var result bytes.Buffer

	for _, line := range strings.Split(message, "\n") {
		result.WriteString(fmt.Sprintf("%s %s: %s\n", arrowText, u.Target, line))
	}

	return strings.TrimRightFunc(result.String(), unicode.IsSpace)
}
