package cli

import (
	"github.com/posener/complete"
)

const (
	RunResultHelp = -18511
)

type Command interface {
	Help() string

	//

	Run(args []string) int

	Synopsis() string
}

type CommandAutocomplete interface {
	AutocompleteArgs() complete.Predictor

	AutocompleteFlags() complete.Flags
}

//
type CommandHelpTemplate interface {

	//

	//
	HelpTemplate() string
}

type CommandFactory func() (Command, error)
