// Package cmd used for command line options for the complete tool
package cmd

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/posener/complete/cmd/install"
)

type CLI struct {
	Name          string
	InstallName   string
	UninstallName string

	install   bool
	uninstall bool
	yes       bool
}

const (
	defaultInstallName   = "install"
	defaultUninstallName = "uninstall"
)

func (f *CLI) Run() bool {
	err := f.validate()
	if err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}

	switch {
	case f.install:
		f.prompt()
		err = install.Install(f.Name)
	case f.uninstall:
		f.prompt()
		err = install.Uninstall(f.Name)
	default:

		return false
	}

	if err != nil {
		fmt.Printf("%s failed! %s\n", f.action(), err)
		os.Exit(3)
	}
	fmt.Println("Done!")
	return true
}

func (f *CLI) prompt() {
	defer fmt.Println(f.action() + "ing...")
	if f.yes {
		return
	}
	fmt.Printf("%s completion for %s? ", f.action(), f.Name)
	var answer string
	fmt.Scanln(&answer)

	switch strings.ToLower(answer) {
	case "y", "yes":
		return
	default:
		fmt.Println("Cancelling...")
		os.Exit(1)
	}
}

func (f *CLI) AddFlags(flags *flag.FlagSet) {
	if flags == nil {
		flags = flag.CommandLine
	}

	if f.InstallName == "" {
		f.InstallName = defaultInstallName
	}
	if f.UninstallName == "" {
		f.UninstallName = defaultUninstallName
	}

	if flags.Lookup(f.InstallName) == nil {
		flags.BoolVar(&f.install, f.InstallName, false,
			fmt.Sprintf("Install completion for %s command", f.Name))
	}
	if flags.Lookup(f.UninstallName) == nil {
		flags.BoolVar(&f.uninstall, f.UninstallName, false,
			fmt.Sprintf("Uninstall completion for %s command", f.Name))
	}
	if flags.Lookup("y") == nil {
		flags.BoolVar(&f.yes, "y", false, "Don't prompt user for typing 'yes'")
	}
}

func (f *CLI) validate() error {
	if f.install && f.uninstall {
		return errors.New("Install and uninstall are mutually exclusive")
	}
	return nil
}

func (f *CLI) action() string {
	switch {
	case f.install:
		return "Install"
	case f.uninstall:
		return "Uninstall"
	default:
		return "unknown"
	}
}
