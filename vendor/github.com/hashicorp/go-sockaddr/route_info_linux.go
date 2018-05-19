package sockaddr

import (
	"errors"
	"os/exec"
)

type routeInfo struct {
	cmds map[string][]string
}

func NewRouteInfo() (routeInfo, error) {

	path, _ := exec.LookPath("ip")
	if path == "" {
		path = "/sbin/ip"
	}

	return routeInfo{
		cmds: map[string][]string{"ip": {path, "route"}},
	}, nil
}

func (ri routeInfo) GetDefaultInterfaceName() (string, error) {
	out, err := exec.Command(ri.cmds["ip"][0], ri.cmds["ip"][1:]...).Output()
	if err != nil {
		return "", err
	}

	var ifName string
	if ifName, err = parseDefaultIfNameFromIPCmd(string(out)); err != nil {
		return "", errors.New("No default interface found")
	}
	return ifName, nil
}
