package cli

import (
	"bytes"
	"fmt"
	"log"
	"sort"
	"strings"
)

type HelpFunc func(map[string]CommandFactory) string

func BasicHelpFunc(app string) HelpFunc {
	return func(commands map[string]CommandFactory) string {
		var buf bytes.Buffer
		buf.WriteString(fmt.Sprintf(
			"Usage: %s [--version] [--help] <command> [<args>]\n\n",
			app))
		buf.WriteString("Available commands are:\n")

		keys := make([]string, 0, len(commands))
		maxKeyLen := 0
		for key := range commands {
			if len(key) > maxKeyLen {
				maxKeyLen = len(key)
			}

			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, key := range keys {
			commandFunc, ok := commands[key]
			if !ok {

				panic("command not found: " + key)
			}

			command, err := commandFunc()
			if err != nil {
				log.Printf("[ERR] cli: Command '%s' failed to load: %s",
					key, err)
				continue
			}

			key = fmt.Sprintf("%s%s", key, strings.Repeat(" ", maxKeyLen-len(key)))
			buf.WriteString(fmt.Sprintf("    %s    %s\n", key, command.Synopsis()))
		}

		return buf.String()
	}
}

func FilteredHelpFunc(include []string, f HelpFunc) HelpFunc {
	return func(commands map[string]CommandFactory) string {
		set := make(map[string]struct{})
		for _, k := range include {
			set[k] = struct{}{}
		}

		filtered := make(map[string]CommandFactory)
		for k, f := range commands {
			if _, ok := set[k]; ok {
				filtered[k] = f
			}
		}

		return f(filtered)
	}
}
