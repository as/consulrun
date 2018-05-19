package sockaddr

import "os/exec"

var cmds map[string][]string = map[string][]string{
	"netstat":  {"netstat", "-rn"},
	"ipconfig": {"ipconfig"},
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
	ifNameOut, err := exec.Command(cmds["netstat"][0], cmds["netstat"][1:]...).Output()
	if err != nil {
		return "", err
	}

	ipconfigOut, err := exec.Command(cmds["ipconfig"][0], cmds["ipconfig"][1:]...).Output()
	if err != nil {
		return "", err
	}

	ifName, err := parseDefaultIfNameWindows(string(ifNameOut), string(ipconfigOut))
	if err != nil {
		return "", err
	}

	return ifName, nil
}
