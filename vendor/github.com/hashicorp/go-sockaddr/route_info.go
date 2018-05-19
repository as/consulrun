package sockaddr

type RouteInterface interface {
	GetDefaultInterfaceName() (string, error)
}

func (ri routeInfo) VisitCommands(fn func(name string, cmd []string)) {
	for k, v := range ri.cmds {
		cmds := append([]string(nil), v...)
		fn(k, cmds)
	}
}
