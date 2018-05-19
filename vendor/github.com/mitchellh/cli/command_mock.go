package cli

import (
	"github.com/posener/complete"
)

type MockCommand struct {
	HelpText     string
	RunResult    int
	SynopsisText string

	RunCalled bool
	RunArgs   []string
}

func (c *MockCommand) Help() string {
	return c.HelpText
}

func (c *MockCommand) Run(args []string) int {
	c.RunCalled = true
	c.RunArgs = args

	return c.RunResult
}

func (c *MockCommand) Synopsis() string {
	return c.SynopsisText
}

type MockCommandAutocomplete struct {
	MockCommand

	AutocompleteArgsValue  complete.Predictor
	AutocompleteFlagsValue complete.Flags
}

func (c *MockCommandAutocomplete) AutocompleteArgs() complete.Predictor {
	return c.AutocompleteArgsValue
}

func (c *MockCommandAutocomplete) AutocompleteFlags() complete.Flags {
	return c.AutocompleteFlagsValue
}

type MockCommandHelpTemplate struct {
	MockCommand

	HelpTemplateText string
}

func (c *MockCommandHelpTemplate) HelpTemplate() string {
	return c.HelpTemplateText
}
