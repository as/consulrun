package cli

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"text/template"

	"github.com/armon/go-radix"
	"github.com/posener/complete"
)

//
//
//
//
//
//
//
type CLI struct {
	Args []string

	//

	Commands map[string]CommandFactory

	Name string

	Version string

	//

	//

	//

	//

	//

	Autocomplete               bool
	AutocompleteInstall        string
	AutocompleteUninstall      string
	AutocompleteNoDefaultFlags bool
	AutocompleteGlobalFlags    complete.Flags
	autocompleteInstaller      autocompleteInstaller // For tests

	//

	//

	HelpFunc   HelpFunc
	HelpWriter io.Writer

	once           sync.Once
	autocomplete   *complete.Complete
	commandTree    *radix.Tree
	commandNested  bool
	subcommand     string
	subcommandArgs []string
	topFlags       []string

	isHelp                  bool
	isVersion               bool
	isAutocompleteInstall   bool
	isAutocompleteUninstall bool
}

func NewCLI(app, version string) *CLI {
	return &CLI{
		Name:         app,
		Version:      version,
		HelpFunc:     BasicHelpFunc(app),
		Autocomplete: true,
	}

}

func (c *CLI) IsHelp() bool {
	c.once.Do(c.init)
	return c.isHelp
}

func (c *CLI) IsVersion() bool {
	c.once.Do(c.init)
	return c.isVersion
}

func (c *CLI) Run() (int, error) {
	c.once.Do(c.init)

	if c.Autocomplete && c.autocomplete.Complete() {
		return 0, nil
	}

	if c.IsVersion() && c.Version != "" {
		c.HelpWriter.Write([]byte(c.Version + "\n"))
		return 0, nil
	}

	if c.IsHelp() && c.Subcommand() == "" {
		c.HelpWriter.Write([]byte(c.HelpFunc(c.helpCommands(c.Subcommand())) + "\n"))
		return 0, nil
	}

	if c.Autocomplete {

		if c.Name == "" {
			return 1, fmt.Errorf(
				"internal error: CLI.Name must be specified for autocomplete to work")
		}

		if c.isAutocompleteInstall && c.isAutocompleteUninstall {
			return 1, fmt.Errorf(
				"Either the autocomplete install or uninstall flag may " +
					"be specified, but not both.")
		}

		if c.isAutocompleteInstall {
			if err := c.autocompleteInstaller.Install(c.Name); err != nil {
				return 1, err
			}

			return 0, nil
		}

		if c.isAutocompleteUninstall {
			if err := c.autocompleteInstaller.Uninstall(c.Name); err != nil {
				return 1, err
			}

			return 0, nil
		}
	}

	raw, ok := c.commandTree.Get(c.Subcommand())
	if !ok {
		c.HelpWriter.Write([]byte(c.HelpFunc(c.helpCommands(c.subcommandParent())) + "\n"))
		return 1, nil
	}

	command, err := raw.(CommandFactory)()
	if err != nil {
		return 1, err
	}

	if c.IsHelp() {
		c.commandHelp(command)
		return 0, nil
	}

	if len(c.topFlags) > 0 {
		c.HelpWriter.Write([]byte(
			"Invalid flags before the subcommand. If these flags are for\n" +
				"the subcommand, please put them after the subcommand.\n\n"))
		c.commandHelp(command)
		return 1, nil
	}

	code := command.Run(c.SubcommandArgs())
	if code == RunResultHelp {

		c.commandHelp(command)
		return 1, nil
	}

	return code, nil
}

func (c *CLI) Subcommand() string {
	c.once.Do(c.init)
	return c.subcommand
}

func (c *CLI) SubcommandArgs() []string {
	c.once.Do(c.init)
	return c.subcommandArgs
}

func (c *CLI) subcommandParent() string {

	sub := c.Subcommand()
	if sub == "" {
		return sub
	}

	sub = strings.TrimRight(sub, " ")
	idx := strings.LastIndex(sub, " ")

	if idx == -1 {

		return ""
	}

	return sub[:idx]
}

func (c *CLI) init() {
	if c.HelpFunc == nil {
		c.HelpFunc = BasicHelpFunc("app")

		if c.Name != "" {
			c.HelpFunc = BasicHelpFunc(c.Name)
		}
	}

	if c.HelpWriter == nil {
		c.HelpWriter = os.Stderr
	}

	c.commandTree = radix.New()
	c.commandNested = false
	for k, v := range c.Commands {
		k = strings.TrimSpace(k)
		c.commandTree.Insert(k, v)
		if strings.ContainsRune(k, ' ') {
			c.commandNested = true
		}
	}

	if c.commandNested {
		var walkFn radix.WalkFn
		toInsert := make(map[string]struct{})
		walkFn = func(k string, raw interface{}) bool {
			idx := strings.LastIndex(k, " ")
			if idx == -1 {

				return false
			}

			k = k[:idx]
			if _, ok := c.commandTree.Get(k); ok {

				return false
			}

			toInsert[k] = struct{}{}

			return walkFn(k, nil)
		}

		c.commandTree.Walk(walkFn)

		for k := range toInsert {
			var f CommandFactory = func() (Command, error) {
				return &MockCommand{
					HelpText:  "This command is accessed by using one of the subcommands below.",
					RunResult: RunResultHelp,
				}, nil
			}

			c.commandTree.Insert(k, f)
		}
	}

	if c.Autocomplete {
		c.initAutocomplete()
	}

	c.processArgs()
}

func (c *CLI) initAutocomplete() {
	if c.AutocompleteInstall == "" {
		c.AutocompleteInstall = defaultAutocompleteInstall
	}

	if c.AutocompleteUninstall == "" {
		c.AutocompleteUninstall = defaultAutocompleteUninstall
	}

	if c.autocompleteInstaller == nil {
		c.autocompleteInstaller = &realAutocompleteInstaller{}
	}

	cmd := c.initAutocompleteSub("")

	if !c.AutocompleteNoDefaultFlags {
		cmd.Flags = map[string]complete.Predictor{
			"-" + c.AutocompleteInstall:   complete.PredictNothing,
			"-" + c.AutocompleteUninstall: complete.PredictNothing,
			"-help":    complete.PredictNothing,
			"-version": complete.PredictNothing,
		}
	}
	cmd.GlobalFlags = c.AutocompleteGlobalFlags

	c.autocomplete = complete.New(c.Name, cmd)
}

func (c *CLI) initAutocompleteSub(prefix string) complete.Command {
	var cmd complete.Command
	walkFn := func(k string, raw interface{}) bool {

		fullKey := k

		if len(prefix) > 0 {

			k = k[len(prefix)+1:]
		}

		if idx := strings.Index(k, " "); idx >= 0 {

			k = k[:idx]
		}

		if _, ok := cmd.Sub[k]; ok {

			return false
		}

		if cmd.Sub == nil {
			cmd.Sub = complete.Commands(make(map[string]complete.Command))
		}
		subCmd := c.initAutocompleteSub(fullKey)

		impl, err := raw.(CommandFactory)()
		if err != nil {
			impl = nil
		}

		if c, ok := impl.(CommandAutocomplete); ok {
			subCmd.Args = c.AutocompleteArgs()
			subCmd.Flags = c.AutocompleteFlags()
		}

		cmd.Sub[k] = subCmd
		return false
	}

	walkPrefix := prefix
	if walkPrefix != "" {
		walkPrefix += " "
	}

	c.commandTree.WalkPrefix(walkPrefix, walkFn)
	return cmd
}

func (c *CLI) commandHelp(command Command) {

	tpl := strings.TrimSpace(defaultHelpTemplate)
	if t, ok := command.(CommandHelpTemplate); ok {
		tpl = t.HelpTemplate()
	}
	if !strings.HasSuffix(tpl, "\n") {
		tpl += "\n"
	}

	t, err := template.New("root").Parse(tpl)
	if err != nil {
		t = template.Must(template.New("root").Parse(fmt.Sprintf(
			"Internal error! Failed to parse command help template: %s\n", err)))
	}

	data := map[string]interface{}{
		"Name": c.Name,
		"Help": command.Help(),
	}

	var subcommandsTpl []map[string]interface{}
	if c.commandNested {

		subcommands := c.helpCommands(c.Subcommand())
		keys := make([]string, 0, len(subcommands))
		for k := range subcommands {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		var longest int
		for _, k := range keys {
			if v := len(k); v > longest {
				longest = v
			}
		}

		subcommandsTpl = make([]map[string]interface{}, 0, len(subcommands))
		for _, k := range keys {

			raw, ok := subcommands[k]
			if !ok {
				c.HelpWriter.Write([]byte(fmt.Sprintf(
					"Error getting subcommand %q", k)))
			}
			sub, err := raw()
			if err != nil {
				c.HelpWriter.Write([]byte(fmt.Sprintf(
					"Error instantiating %q: %s", k, err)))
			}

			name := k
			if idx := strings.LastIndex(k, " "); idx > -1 {
				name = name[idx+1:]
			}

			subcommandsTpl = append(subcommandsTpl, map[string]interface{}{
				"Name":        name,
				"NameAligned": name + strings.Repeat(" ", longest-len(k)),
				"Help":        sub.Help(),
				"Synopsis":    sub.Synopsis(),
			})
		}
	}
	data["Subcommands"] = subcommandsTpl

	err = t.Execute(c.HelpWriter, data)
	if err == nil {
		return
	}

	c.HelpWriter.Write([]byte(fmt.Sprintf(
		"Internal error rendering help: %s", err)))
}

func (c *CLI) helpCommands(prefix string) map[string]CommandFactory {

	if prefix != "" && prefix[len(prefix)-1] != ' ' {
		prefix += " "
	}

	var keys []string
	c.commandTree.WalkPrefix(prefix, func(k string, raw interface{}) bool {

		if !strings.Contains(k[len(prefix):], " ") {
			keys = append(keys, k)
		}

		return false
	})

	result := make(map[string]CommandFactory, len(keys))
	for _, k := range keys {
		raw, ok := c.commandTree.Get(k)
		if !ok {

			panic("not found: " + k)
		}

		result[k] = raw.(CommandFactory)
	}

	return result
}

func (c *CLI) processArgs() {
	for i, arg := range c.Args {
		if arg == "--" {
			break
		}

		if arg == "-h" || arg == "-help" || arg == "--help" {
			c.isHelp = true
			continue
		}

		if c.Autocomplete {
			if arg == "-"+c.AutocompleteInstall || arg == "--"+c.AutocompleteInstall {
				c.isAutocompleteInstall = true
				continue
			}

			if arg == "-"+c.AutocompleteUninstall || arg == "--"+c.AutocompleteUninstall {
				c.isAutocompleteUninstall = true
				continue
			}
		}

		if c.subcommand == "" {

			if arg == "-v" || arg == "-version" || arg == "--version" {
				c.isVersion = true
				continue
			}

			if arg != "" && arg[0] == '-' {

				c.topFlags = append(c.topFlags, arg)
			}
		}

		if c.subcommand == "" && arg != "" && arg[0] != '-' {
			c.subcommand = arg
			if c.commandNested {

				searchKey := strings.Join(c.Args[i:], " ")
				k, _, ok := c.commandTree.LongestPrefix(searchKey)
				if ok {

					reVerify := regexp.MustCompile(regexp.QuoteMeta(k) + `( |$)`)
					if reVerify.MatchString(searchKey) {
						c.subcommand = k
						i += strings.Count(k, " ")
					}
				}
			}

			c.subcommandArgs = c.Args[i+1:]
		}
	}

	if c.subcommand == "" {
		if _, ok := c.Commands[""]; ok {
			args := c.topFlags
			args = append(args, c.subcommandArgs...)
			c.topFlags = nil
			c.subcommandArgs = args
		}
	}
}

const defaultAutocompleteInstall = "autocomplete-install"
const defaultAutocompleteUninstall = "autocomplete-uninstall"

const defaultHelpTemplate = `
{{.Help}}{{if gt (len .Subcommands) 0}}

Subcommands:
{{- range $value := .Subcommands }}
    {{ $value.NameAligned }}    {{ $value.Synopsis }}{{ end }}
{{- end }}
`
