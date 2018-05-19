package dns

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"
)

type ClientConfig struct {
	Servers  []string // servers to use
	Search   []string // suffixes to append to local name
	Port     string   // what port to use
	Ndots    int      // number of dots in name to trigger absolute lookup
	Timeout  int      // seconds before giving up on packet
	Attempts int      // lost packets before giving up on server, not used in the package dns
}

func ClientConfigFromFile(resolvconf string) (*ClientConfig, error) {
	file, err := os.Open(resolvconf)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ClientConfigFromReader(file)
}

func ClientConfigFromReader(resolvconf io.Reader) (*ClientConfig, error) {
	c := new(ClientConfig)
	scanner := bufio.NewScanner(resolvconf)
	c.Servers = make([]string, 0)
	c.Search = make([]string, 0)
	c.Port = "53"
	c.Ndots = 1
	c.Timeout = 5
	c.Attempts = 2

	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, err
		}
		line := scanner.Text()
		f := strings.Fields(line)
		if len(f) < 1 {
			continue
		}
		switch f[0] {
		case "nameserver": // add one name server
			if len(f) > 1 {

				name := f[1]
				c.Servers = append(c.Servers, name)
			}

		case "domain": // set search path to just this domain
			if len(f) > 1 {
				c.Search = make([]string, 1)
				c.Search[0] = f[1]
			} else {
				c.Search = make([]string, 0)
			}

		case "search": // set search path to given servers
			c.Search = make([]string, len(f)-1)
			for i := 0; i < len(c.Search); i++ {
				c.Search[i] = f[i+1]
			}

		case "options": // magic options
			for i := 1; i < len(f); i++ {
				s := f[i]
				switch {
				case len(s) >= 6 && s[:6] == "ndots:":
					n, _ := strconv.Atoi(s[6:])
					if n < 0 {
						n = 0
					} else if n > 15 {
						n = 15
					}
					c.Ndots = n
				case len(s) >= 8 && s[:8] == "timeout:":
					n, _ := strconv.Atoi(s[8:])
					if n < 1 {
						n = 1
					}
					c.Timeout = n
				case len(s) >= 8 && s[:9] == "attempts:":
					n, _ := strconv.Atoi(s[9:])
					if n < 1 {
						n = 1
					}
					c.Attempts = n
				case s == "rotate":
					/* not imp */
				}
			}
		}
	}
	return c, nil
}

// config. It is based off of go's net/dns name building, but it does not
func (c *ClientConfig) NameList(name string) []string {

	if IsFqdn(name) {
		return []string{name}
	}

	hasNdots := CountLabel(name) > c.Ndots

	name = Fqdn(name)

	names := []string{}

	if hasNdots {
		names = append(names, name)
	}
	for _, s := range c.Search {
		names = append(names, Fqdn(name+s))
	}

	if !hasNdots {
		names = append(names, name)
	}
	return names
}
