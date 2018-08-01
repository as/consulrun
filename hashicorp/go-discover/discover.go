// Package discover provides functions to get metadata for different
package discover

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	"github.com/as/consulrun/hashicorp/go-discover/provider/os"
)

type Provider interface {
	Addrs(args map[string]string, l *log.Logger) ([]string, error)

	Help() string
}

var Providers = map[string]Provider{
	"os": &os.Provider{},
}

type Discover struct {
	Providers map[string]Provider

	once sync.Once
}

func (d *Discover) initProviders() {
	if d.Providers == nil {
		d.Providers = Providers
	}
}

func (d *Discover) Names() []string {
	d.once.Do(d.initProviders)

	var names []string
	for n := range d.Providers {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

var globalHelp = `The options for discovering ip addresses are provided as a
  single string value in "key=value key=value ..." format where
  the values are URL encoded.

    provider=aws region=eu-west-1 ...

  The options are provider specific and are listed below.
`

func (d *Discover) Help() string {
	d.once.Do(d.initProviders)

	h := []string{globalHelp}
	for _, name := range d.Names() {
		h = append(h, d.Providers[name].Help())
	}
	return strings.Join(h, "\n")
}

func (d *Discover) Addrs(cfg string, l *log.Logger) ([]string, error) {
	d.once.Do(d.initProviders)

	args, err := Parse(cfg)
	if err != nil {
		return nil, fmt.Errorf("discover: %s", err)
	}

	name := args["provider"]
	if name == "" {
		return nil, fmt.Errorf("discover: no provider")
	}

	providers := d.Providers
	if providers == nil {
		providers = Providers
	}

	p := providers[name]
	if p == nil {
		return nil, fmt.Errorf("discover: unknown provider " + name)
	}
	l.Printf("[DEBUG] discover: Using provider %q", name)

	return p.Addrs(args, l)
}
