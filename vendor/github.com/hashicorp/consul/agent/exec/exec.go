package exec

import (
	"fmt"
	"os/exec"
)

func Subprocess(args []string) (*exec.Cmd, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("need an executable to run")
	}
	return exec.Command(args[0], args[1:]...), nil
}
