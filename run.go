package consulrun

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/as/consulrun/hashicorp/consul/command"
	"github.com/as/consulrun/hashicorp/consul/lib"
	"github.com/mitchellh/cli"
)

func init() {
	lib.SeedMathRand()
}

type Config struct {
	Stdout io.Writer
	Stderr io.Writer
}

var DefaultConfig = Config{
	Stdout: os.Stdout,
	Stderr: os.Stderr,
}

// Cmd runs the given consul subcommand with an optional Config
// For example, "consul agent -help" would be Cmd(nil, "agent", "help")
// If conf is nil, the cmd outputs to stdout and stderr
func Cmd(conf *Config, args ...string) int {
	log.SetOutput(ioutil.Discard)
	if conf == nil {
		conf = &DefaultConfig
	}

	for _, arg := range args {
		if arg == "--" {
			break
		}

		if arg == "-v" || arg == "--version" {
			args = []string{"version"}
			break
		}
	}

	ui := &cli.BasicUi{Writer: conf.Stdout, ErrorWriter: conf.Stderr}
	cmds := Map(ui)
	var names []string
	for c := range cmds {
		names = append(names, c)
	}

	cli := &cli.CLI{
		Args:         args,
		Commands:     cmds,
		Autocomplete: true,
		Name:         "consul",
		HelpFunc:     cli.FilteredHelpFunc(names, cli.BasicHelpFunc("consul")),
	}

	//	for k, v := range cmds {
	//cli.Commands[k] = crap.CommandFactory(v)
	//	}

	exitCode, err := cli.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error executing CLI: %s\n", err.Error())
		return 1
	}

	return exitCode
}

func Map(ui cli.Ui) map[string]cli.CommandFactory {
	m := make(map[string]cli.CommandFactory)
	for name, fn := range command.Reg() {
		thisFn := fn
		m[name] = func() (cli.Command, error) {
			return thisFn(ui)
		}
	}
	return m
}
