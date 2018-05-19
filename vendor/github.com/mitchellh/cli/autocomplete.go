package cli

import (
	"github.com/posener/complete/cmd/install"
)

//
type autocompleteInstaller interface {
	Install(string) error
	Uninstall(string) error
}

type realAutocompleteInstaller struct{}

func (i *realAutocompleteInstaller) Install(cmd string) error {
	return install.Install(cmd)
}

func (i *realAutocompleteInstaller) Uninstall(cmd string) error {
	return install.Uninstall(cmd)
}

type mockAutocompleteInstaller struct {
	InstallCalled   bool
	UninstallCalled bool
}

func (i *mockAutocompleteInstaller) Install(cmd string) error {
	i.InstallCalled = true
	return nil
}

func (i *mockAutocompleteInstaller) Uninstall(cmd string) error {
	i.UninstallCalled = true
	return nil
}
