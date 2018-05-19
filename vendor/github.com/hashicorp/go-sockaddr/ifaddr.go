package sockaddr

import "strings"

var ifAddrAttrMap map[AttrName]func(IfAddr) string
var ifAddrAttrs []AttrName

func init() {
	ifAddrAttrInit()
}

//
func GetPrivateIP() (string, error) {
	privateIfs, err := GetPrivateInterfaces()
	if err != nil {
		return "", err
	}
	if len(privateIfs) < 1 {
		return "", nil
	}

	ifAddr := privateIfs[0]
	ip := *ToIPAddr(ifAddr.SockAddr)
	return ip.NetIP().String(), nil
}

//
func GetPrivateIPs() (string, error) {
	ifAddrs, err := GetAllInterfaces()
	if err != nil {
		return "", err
	} else if len(ifAddrs) < 1 {
		return "", nil
	}

	ifAddrs, _ = FilterIfByType(ifAddrs, TypeIP)
	if len(ifAddrs) == 0 {
		return "", nil
	}

	OrderedIfAddrBy(AscIfType, AscIfNetworkSize).Sort(ifAddrs)

	ifAddrs, _, err = IfByRFC("6890", ifAddrs)
	if err != nil {
		return "", err
	} else if len(ifAddrs) == 0 {
		return "", nil
	}

	_, ifAddrs, err = IfByRFC(ForwardingBlacklistRFC, ifAddrs)
	if err != nil {
		return "", err
	} else if len(ifAddrs) == 0 {
		return "", nil
	}

	ips := make([]string, 0, len(ifAddrs))
	for _, ifAddr := range ifAddrs {
		ip := *ToIPAddr(ifAddr.SockAddr)
		s := ip.NetIP().String()
		ips = append(ips, s)
	}

	return strings.Join(ips, " "), nil
}

//
func GetPublicIP() (string, error) {
	publicIfs, err := GetPublicInterfaces()
	if err != nil {
		return "", err
	} else if len(publicIfs) < 1 {
		return "", nil
	}

	ifAddr := publicIfs[0]
	ip := *ToIPAddr(ifAddr.SockAddr)
	return ip.NetIP().String(), nil
}

//
func GetPublicIPs() (string, error) {
	ifAddrs, err := GetAllInterfaces()
	if err != nil {
		return "", err
	} else if len(ifAddrs) < 1 {
		return "", nil
	}

	ifAddrs, _ = FilterIfByType(ifAddrs, TypeIP)
	if len(ifAddrs) == 0 {
		return "", nil
	}

	OrderedIfAddrBy(AscIfType, AscIfNetworkSize).Sort(ifAddrs)

	_, ifAddrs, err = IfByRFC("6890", ifAddrs)
	if err != nil {
		return "", err
	} else if len(ifAddrs) == 0 {
		return "", nil
	}

	ips := make([]string, 0, len(ifAddrs))
	for _, ifAddr := range ifAddrs {
		ip := *ToIPAddr(ifAddr.SockAddr)
		s := ip.NetIP().String()
		ips = append(ips, s)
	}

	return strings.Join(ips, " "), nil
}

//
func GetInterfaceIP(namedIfRE string) (string, error) {
	ifAddrs, err := GetAllInterfaces()
	if err != nil {
		return "", err
	}

	ifAddrs, _, err = IfByName(namedIfRE, ifAddrs)
	if err != nil {
		return "", err
	}

	ifAddrs, _, err = IfByFlag("forwardable", ifAddrs)
	if err != nil {
		return "", err
	}

	ifAddrs, err = SortIfBy("+type,+size", ifAddrs)
	if err != nil {
		return "", err
	}

	if len(ifAddrs) == 0 {
		return "", err
	}

	ip := ToIPAddr(ifAddrs[0].SockAddr)
	if ip == nil {
		return "", err
	}

	return IPAddrAttr(*ip, "address"), nil
}

//
func GetInterfaceIPs(namedIfRE string) (string, error) {
	ifAddrs, err := GetAllInterfaces()
	if err != nil {
		return "", err
	}

	ifAddrs, _, err = IfByName(namedIfRE, ifAddrs)
	if err != nil {
		return "", err
	}

	ifAddrs, err = SortIfBy("+type,+size", ifAddrs)
	if err != nil {
		return "", err
	}

	if len(ifAddrs) == 0 {
		return "", err
	}

	ips := make([]string, 0, len(ifAddrs))
	for _, ifAddr := range ifAddrs {
		ip := *ToIPAddr(ifAddr.SockAddr)
		s := ip.NetIP().String()
		ips = append(ips, s)
	}

	return strings.Join(ips, " "), nil
}

func IfAddrAttrs() []AttrName {
	return ifAddrAttrs
}

func IfAddrAttr(ifAddr IfAddr, attrName AttrName) string {
	fn, found := ifAddrAttrMap[attrName]
	if !found {
		return ""
	}

	return fn(ifAddr)
}

func ifAddrAttrInit() {

	ifAddrAttrs = []AttrName{
		"flags",
		"name",
	}

	ifAddrAttrMap = map[AttrName]func(ifAddr IfAddr) string{
		"flags": func(ifAddr IfAddr) string {
			return ifAddr.Interface.Flags.String()
		},
		"name": func(ifAddr IfAddr) string {
			return ifAddr.Interface.Name
		},
	}
}
