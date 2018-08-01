package sockaddr

import (
	"errors"
	"os/exec"
)

var cmds map[string][]string = map[string][]string{
	"route": {"/usr/sbin/route", "-n", "get", "default"},
}

type routeInfo struct {
	cmds map[string][]string
}

func NewRouteInfo() (routeInfo, error) {
	return routeInfo{
		cmds: cmds,
	}, nil
}

func (ri routeInfo) GetDefaultInterfaceName() (string, error) {
	out, err := exec.Command(cmds["route"][0], cmds["route"][1:]...).Output()
	if err != nil {
		return "", err
	}

	var ifName string
	if ifName, err = parseDefaultIfNameFromRoute(string(out)); err != nil {
		return "", errors.New("No default interface found")
	}
	return ifName, nil
}
