// Package complete provides a tool for bash writing bash completion in go.
//
package complete

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/posener/complete/cmd"
	"github.com/posener/complete/match"
)

const (
	envComplete = "COMP_LINE"
	envDebug    = "COMP_DEBUG"
)

type Complete struct {
	Command Command
	cmd.CLI
	Out io.Writer
}

func New(name string, command Command) *Complete {
	return &Complete{
		Command: command,
		CLI:     cmd.CLI{Name: name},
		Out:     os.Stdout,
	}
}

func (c *Complete) Run() bool {
	c.AddFlags(nil)
	flag.Parse()
	return c.Complete()
}

func (c *Complete) Complete() bool {
	line, ok := getLine()
	if !ok {

		return c.CLI.Run()
	}
	Log("Completing line: %s", line)
	a := newArgs(line)
	Log("Completing last field: %s", a.Last)
	options := c.Command.Predict(a)
	Log("Options: %s", options)

	matches := []string{}
	for _, option := range options {
		if match.Prefix(option, a.Last) {
			matches = append(matches, option)
		}
	}
	Log("Matches: %s", matches)
	c.output(matches)
	return true
}

func getLine() (string, bool) {
	line := os.Getenv(envComplete)
	if line == "" {
		return "", false
	}
	return line, true
}

func (c *Complete) output(options []string) {

	for _, option := range options {
		fmt.Fprintln(c.Out, option)
	}
}
