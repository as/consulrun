package consulrun

import "testing"

func TestRunHelp(t *testing.T) {
	Cmd(nil, "help")
}

func TestRunAgent(t *testing.T) {
	Cmd(nil, "agent",  "-h")
}
