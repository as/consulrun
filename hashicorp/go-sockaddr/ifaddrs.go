package sockaddr

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var (
	signRE       *regexp.Regexp = regexp.MustCompile(`^[\s]*[+-]`)
	whitespaceRE *regexp.Regexp = regexp.MustCompile(`[\s]+`)
	ifNameRE     *regexp.Regexp = regexp.MustCompile(`^Ethernet adapter ([^\s:]+):`)
	ipAddrRE     *regexp.Regexp = regexp.MustCompile(`^   IPv[46] Address\. \. \. \. \. \. \. \. \. \. \. : ([^\s]+)`)
)

type IfAddrs []IfAddr

func (ifs IfAddrs) Len() int { return len(ifs) }

type CmpIfAddrFunc func(p1, p2 *IfAddr) int

type multiIfAddrSorter struct {
	ifAddrs IfAddrs
	cmp     []CmpIfAddrFunc
}

func (ms *multiIfAddrSorter) Sort(ifAddrs IfAddrs) {
	ms.ifAddrs = ifAddrs
	sort.Sort(ms)
}

func OrderedIfAddrBy(cmpFuncs ...CmpIfAddrFunc) *multiIfAddrSorter {
	return &multiIfAddrSorter{
		cmp: cmpFuncs,
	}
}

func (ms *multiIfAddrSorter) Len() int {
	return len(ms.ifAddrs)
}

func (ms *multiIfAddrSorter) Less(i, j int) bool {
	p, q := &ms.ifAddrs[i], &ms.ifAddrs[j]

	var k int
	for k = 0; k < len(ms.cmp)-1; k++ {
		cmp := ms.cmp[k]
		x := cmp(p, q)
		switch x {
		case -1:

			return true
		case 1:

			return false
		}

	}

	switch ms.cmp[k](p, q) {
	case -1:
		return true
	case 1:
		return false
	default:

		return false
		panic("undefined sort order for remaining items in the list")
	}
}

func (ms *multiIfAddrSorter) Swap(i, j int) {
	ms.ifAddrs[i], ms.ifAddrs[j] = ms.ifAddrs[j], ms.ifAddrs[i]
}

func AscIfAddress(p1Ptr, p2Ptr *IfAddr) int {
	return AscAddress(&p1Ptr.SockAddr, &p2Ptr.SockAddr)
}

//
func AscIfDefault(p1Ptr, p2Ptr *IfAddr) int {
	ri, err := NewRouteInfo()
	if err != nil {
		return sortDeferDecision
	}

	defaultIfName, err := ri.GetDefaultInterfaceName()
	if err != nil {
		return sortDeferDecision
	}

	switch {
	case p1Ptr.Interface.Name == defaultIfName && p2Ptr.Interface.Name == defaultIfName:
		return sortDeferDecision
	case p1Ptr.Interface.Name == defaultIfName:
		return sortReceiverBeforeArg
	case p2Ptr.Interface.Name == defaultIfName:
		return sortArgBeforeReceiver
	default:
		return sortDeferDecision
	}
}

func AscIfName(p1Ptr, p2Ptr *IfAddr) int {
	return strings.Compare(p1Ptr.Name, p2Ptr.Name)
}

func AscIfNetworkSize(p1Ptr, p2Ptr *IfAddr) int {
	return AscNetworkSize(&p1Ptr.SockAddr, &p2Ptr.SockAddr)
}

func AscIfPort(p1Ptr, p2Ptr *IfAddr) int {
	return AscPort(&p1Ptr.SockAddr, &p2Ptr.SockAddr)
}

func AscIfPrivate(p1Ptr, p2Ptr *IfAddr) int {
	return AscPrivate(&p1Ptr.SockAddr, &p2Ptr.SockAddr)
}

func AscIfType(p1Ptr, p2Ptr *IfAddr) int {
	return AscType(&p1Ptr.SockAddr, &p2Ptr.SockAddr)
}

func DescIfAddress(p1Ptr, p2Ptr *IfAddr) int {
	return -1 * AscAddress(&p1Ptr.SockAddr, &p2Ptr.SockAddr)
}

func DescIfDefault(p1Ptr, p2Ptr *IfAddr) int {
	return -1 * AscIfDefault(p1Ptr, p2Ptr)
}

func DescIfName(p1Ptr, p2Ptr *IfAddr) int {
	return -1 * strings.Compare(p1Ptr.Name, p2Ptr.Name)
}

func DescIfNetworkSize(p1Ptr, p2Ptr *IfAddr) int {
	return -1 * AscNetworkSize(&p1Ptr.SockAddr, &p2Ptr.SockAddr)
}

func DescIfPort(p1Ptr, p2Ptr *IfAddr) int {
	return -1 * AscPort(&p1Ptr.SockAddr, &p2Ptr.SockAddr)
}

func DescIfPrivate(p1Ptr, p2Ptr *IfAddr) int {
	return -1 * AscPrivate(&p1Ptr.SockAddr, &p2Ptr.SockAddr)
}

func DescIfType(p1Ptr, p2Ptr *IfAddr) int {
	return -1 * AscType(&p1Ptr.SockAddr, &p2Ptr.SockAddr)
}

func FilterIfByType(ifAddrs IfAddrs, type_ SockAddrType) (matchedIfs, excludedIfs IfAddrs) {
	excludedIfs = make(IfAddrs, 0, len(ifAddrs))
	matchedIfs = make(IfAddrs, 0, len(ifAddrs))

	for _, ifAddr := range ifAddrs {
		if ifAddr.SockAddr.Type()&type_ != 0 {
			matchedIfs = append(matchedIfs, ifAddr)
		} else {
			excludedIfs = append(excludedIfs, ifAddr)
		}
	}
	return matchedIfs, excludedIfs
}

func IfAttr(selectorName string, ifAddr IfAddr) (string, error) {
	attrName := AttrName(strings.ToLower(selectorName))
	attrVal, err := ifAddr.Attr(attrName)
	return attrVal, err
}

func IfAttrs(selectorName string, ifAddrs IfAddrs) (string, error) {
	if len(ifAddrs) == 0 {
		return "", nil
	}

	attrName := AttrName(strings.ToLower(selectorName))
	attrVal, err := ifAddrs[0].Attr(attrName)
	return attrVal, err
}

func GetAllInterfaces() (IfAddrs, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	ifAddrs := make(IfAddrs, 0, len(ifs))
	for _, intf := range ifs {
		addrs, err := intf.Addrs()
		if err != nil {
			return nil, err
		}

		for _, addr := range addrs {
			var ipAddr IPAddr
			ipAddr, err = NewIPAddr(addr.String())
			if err != nil {
				return IfAddrs{}, fmt.Errorf("unable to create an IP address from %q", addr.String())
			}

			ifAddr := IfAddr{
				SockAddr:  ipAddr,
				Interface: intf,
			}
			ifAddrs = append(ifAddrs, ifAddr)
		}
	}

	return ifAddrs, nil
}

func GetDefaultInterfaces() (IfAddrs, error) {
	ri, err := NewRouteInfo()
	if err != nil {
		return nil, err
	}

	defaultIfName, err := ri.GetDefaultInterfaceName()
	if err != nil {
		return nil, err
	}

	var defaultIfs, ifAddrs IfAddrs
	ifAddrs, err = GetAllInterfaces()
	for _, ifAddr := range ifAddrs {
		if ifAddr.Name == defaultIfName {
			defaultIfs = append(defaultIfs, ifAddr)
		}
	}

	return defaultIfs, nil
}

//
func GetPrivateInterfaces() (IfAddrs, error) {
	privateIfs, err := GetAllInterfaces()
	if err != nil {
		return IfAddrs{}, err
	}
	if len(privateIfs) == 0 {
		return IfAddrs{}, nil
	}

	privateIfs, _ = FilterIfByType(privateIfs, TypeIP)
	if len(privateIfs) == 0 {
		return IfAddrs{}, nil
	}

	privateIfs, _, err = IfByFlag("forwardable", privateIfs)
	if err != nil {
		return IfAddrs{}, err
	}

	privateIfs, _, err = IfByFlag("up", privateIfs)
	if err != nil {
		return IfAddrs{}, err
	}

	if len(privateIfs) == 0 {
		return IfAddrs{}, nil
	}

	OrderedIfAddrBy(AscIfDefault, AscIfType, AscIfNetworkSize).Sort(privateIfs)

	privateIfs, _, err = IfByRFC("6890", privateIfs)
	if err != nil {
		return IfAddrs{}, err
	} else if len(privateIfs) == 0 {
		return IfAddrs{}, nil
	}

	return privateIfs, nil
}

//
func GetPublicInterfaces() (IfAddrs, error) {
	publicIfs, err := GetAllInterfaces()
	if err != nil {
		return IfAddrs{}, err
	}
	if len(publicIfs) == 0 {
		return IfAddrs{}, nil
	}

	publicIfs, _ = FilterIfByType(publicIfs, TypeIP)
	if len(publicIfs) == 0 {
		return IfAddrs{}, nil
	}

	publicIfs, _, err = IfByFlag("forwardable", publicIfs)
	if err != nil {
		return IfAddrs{}, err
	}

	publicIfs, _, err = IfByFlag("up", publicIfs)
	if err != nil {
		return IfAddrs{}, err
	}

	if len(publicIfs) == 0 {
		return IfAddrs{}, nil
	}

	OrderedIfAddrBy(AscIfDefault, AscIfType, AscIfNetworkSize).Sort(publicIfs)

	_, publicIfs, err = IfByRFC("6890", publicIfs)
	if err != nil {
		return IfAddrs{}, err
	} else if len(publicIfs) == 0 {
		return IfAddrs{}, nil
	}

	return publicIfs, nil
}

func IfByAddress(inputRe string, ifAddrs IfAddrs) (matched, remainder IfAddrs, err error) {
	re, err := regexp.Compile(inputRe)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to compile address regexp %+q: %v", inputRe, err)
	}

	matchedAddrs := make(IfAddrs, 0, len(ifAddrs))
	excludedAddrs := make(IfAddrs, 0, len(ifAddrs))
	for _, addr := range ifAddrs {
		if re.MatchString(addr.SockAddr.String()) {
			matchedAddrs = append(matchedAddrs, addr)
		} else {
			excludedAddrs = append(excludedAddrs, addr)
		}
	}

	return matchedAddrs, excludedAddrs, nil
}

func IfByName(inputRe string, ifAddrs IfAddrs) (matched, remainder IfAddrs, err error) {
	re, err := regexp.Compile(inputRe)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to compile name regexp %+q: %v", inputRe, err)
	}

	matchedAddrs := make(IfAddrs, 0, len(ifAddrs))
	excludedAddrs := make(IfAddrs, 0, len(ifAddrs))
	for _, addr := range ifAddrs {
		if re.MatchString(addr.Name) {
			matchedAddrs = append(matchedAddrs, addr)
		} else {
			excludedAddrs = append(excludedAddrs, addr)
		}
	}

	return matchedAddrs, excludedAddrs, nil
}

func IfByPort(inputRe string, ifAddrs IfAddrs) (matchedIfs, excludedIfs IfAddrs, err error) {
	re, err := regexp.Compile(inputRe)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to compile port regexp %+q: %v", inputRe, err)
	}

	ipIfs, nonIfs := FilterIfByType(ifAddrs, TypeIP)
	matchedIfs = make(IfAddrs, 0, len(ipIfs))
	excludedIfs = append(IfAddrs(nil), nonIfs...)
	for _, addr := range ipIfs {
		ipAddr := ToIPAddr(addr.SockAddr)
		if ipAddr == nil {
			continue
		}

		port := strconv.FormatInt(int64((*ipAddr).IPPort()), 10)
		if re.MatchString(port) {
			matchedIfs = append(matchedIfs, addr)
		} else {
			excludedIfs = append(excludedIfs, addr)
		}
	}

	return matchedIfs, excludedIfs, nil
}

func IfByRFC(selectorParam string, ifAddrs IfAddrs) (matched, remainder IfAddrs, err error) {
	inputRFC, err := strconv.ParseUint(selectorParam, 10, 64)
	if err != nil {
		return IfAddrs{}, IfAddrs{}, fmt.Errorf("unable to parse RFC number %q: %v", selectorParam, err)
	}

	matchedIfAddrs := make(IfAddrs, 0, len(ifAddrs))
	remainingIfAddrs := make(IfAddrs, 0, len(ifAddrs))

	rfcNetMap := KnownRFCs()
	rfcNets, ok := rfcNetMap[uint(inputRFC)]
	if !ok {
		return nil, nil, fmt.Errorf("unsupported RFC %d", inputRFC)
	}

	for _, ifAddr := range ifAddrs {
		var contained bool
		for _, rfcNet := range rfcNets {
			if rfcNet.Contains(ifAddr.SockAddr) {
				matchedIfAddrs = append(matchedIfAddrs, ifAddr)
				contained = true
				break
			}
		}
		if !contained {
			remainingIfAddrs = append(remainingIfAddrs, ifAddr)
		}
	}

	return matchedIfAddrs, remainingIfAddrs, nil
}

func IfByRFCs(selectorParam string, ifAddrs IfAddrs) (matched, remainder IfAddrs, err error) {
	var includedIfs, excludedIfs IfAddrs
	for _, rfcStr := range strings.Split(selectorParam, "|") {
		includedRFCIfs, excludedRFCIfs, err := IfByRFC(rfcStr, ifAddrs)
		if err != nil {
			return IfAddrs{}, IfAddrs{}, fmt.Errorf("unable to lookup RFC number %q: %v", rfcStr, err)
		}
		includedIfs = append(includedIfs, includedRFCIfs...)
		excludedIfs = append(excludedIfs, excludedRFCIfs...)
	}

	return includedIfs, excludedIfs, nil
}

func IfByMaskSize(selectorParam string, ifAddrs IfAddrs) (matchedIfs, excludedIfs IfAddrs, err error) {
	maskSize, err := strconv.ParseUint(selectorParam, 10, 64)
	if err != nil {
		return IfAddrs{}, IfAddrs{}, fmt.Errorf("invalid exclude size argument (%q): %v", selectorParam, err)
	}

	ipIfs, nonIfs := FilterIfByType(ifAddrs, TypeIP)
	matchedIfs = make(IfAddrs, 0, len(ipIfs))
	excludedIfs = append(IfAddrs(nil), nonIfs...)
	for _, addr := range ipIfs {
		ipAddr := ToIPAddr(addr.SockAddr)
		if ipAddr == nil {
			return IfAddrs{}, IfAddrs{}, fmt.Errorf("unable to filter mask sizes on non-IP type %s: %v", addr.SockAddr.Type().String(), addr.SockAddr.String())
		}

		switch {
		case (*ipAddr).Type()&TypeIPv4 != 0 && maskSize > 32:
			return IfAddrs{}, IfAddrs{}, fmt.Errorf("mask size out of bounds for IPv4 address: %d", maskSize)
		case (*ipAddr).Type()&TypeIPv6 != 0 && maskSize > 128:
			return IfAddrs{}, IfAddrs{}, fmt.Errorf("mask size out of bounds for IPv6 address: %d", maskSize)
		}

		if (*ipAddr).Maskbits() == int(maskSize) {
			matchedIfs = append(matchedIfs, addr)
		} else {
			excludedIfs = append(excludedIfs, addr)
		}
	}

	return matchedIfs, excludedIfs, nil
}

//
//
func IfByType(inputTypes string, ifAddrs IfAddrs) (matched, remainder IfAddrs, err error) {
	matchingIfAddrs := make(IfAddrs, 0, len(ifAddrs))
	remainingIfAddrs := make(IfAddrs, 0, len(ifAddrs))

	ifTypes := strings.Split(strings.ToLower(inputTypes), "|")
	for _, ifType := range ifTypes {
		switch ifType {
		case "ip", "ipv4", "ipv6", "unix":

		default:
			return nil, nil, fmt.Errorf("unsupported type %q %q", ifType, inputTypes)
		}
	}

	for _, ifAddr := range ifAddrs {
		for _, ifType := range ifTypes {
			var matched bool
			switch {
			case ifType == "ip" && ifAddr.SockAddr.Type()&TypeIP != 0:
				matched = true
			case ifType == "ipv4" && ifAddr.SockAddr.Type()&TypeIPv4 != 0:
				matched = true
			case ifType == "ipv6" && ifAddr.SockAddr.Type()&TypeIPv6 != 0:
				matched = true
			case ifType == "unix" && ifAddr.SockAddr.Type()&TypeUnix != 0:
				matched = true
			}

			if matched {
				matchingIfAddrs = append(matchingIfAddrs, ifAddr)
			} else {
				remainingIfAddrs = append(remainingIfAddrs, ifAddr)
			}
		}
	}

	return matchingIfAddrs, remainingIfAddrs, nil
}

//
//
func IfByFlag(inputFlags string, ifAddrs IfAddrs) (matched, remainder IfAddrs, err error) {
	matchedAddrs := make(IfAddrs, 0, len(ifAddrs))
	excludedAddrs := make(IfAddrs, 0, len(ifAddrs))

	var wantForwardable,
		wantGlobalUnicast,
		wantInterfaceLocalMulticast,
		wantLinkLocalMulticast,
		wantLinkLocalUnicast,
		wantLoopback,
		wantMulticast,
		wantUnspecified bool
	var ifFlags net.Flags
	var checkFlags, checkAttrs bool
	for _, flagName := range strings.Split(strings.ToLower(inputFlags), "|") {
		switch flagName {
		case "broadcast":
			checkFlags = true
			ifFlags = ifFlags | net.FlagBroadcast
		case "down":
			checkFlags = true
			ifFlags = (ifFlags &^ net.FlagUp)
		case "forwardable":
			checkAttrs = true
			wantForwardable = true
		case "global unicast":
			checkAttrs = true
			wantGlobalUnicast = true
		case "interface-local multicast":
			checkAttrs = true
			wantInterfaceLocalMulticast = true
		case "link-local multicast":
			checkAttrs = true
			wantLinkLocalMulticast = true
		case "link-local unicast":
			checkAttrs = true
			wantLinkLocalUnicast = true
		case "loopback":
			checkAttrs = true
			checkFlags = true
			ifFlags = ifFlags | net.FlagLoopback
			wantLoopback = true
		case "multicast":
			checkAttrs = true
			checkFlags = true
			ifFlags = ifFlags | net.FlagMulticast
			wantMulticast = true
		case "point-to-point":
			checkFlags = true
			ifFlags = ifFlags | net.FlagPointToPoint
		case "unspecified":
			checkAttrs = true
			wantUnspecified = true
		case "up":
			checkFlags = true
			ifFlags = ifFlags | net.FlagUp
		default:
			return nil, nil, fmt.Errorf("Unknown interface flag: %+q", flagName)
		}
	}

	for _, ifAddr := range ifAddrs {
		var matched bool
		if checkFlags && ifAddr.Interface.Flags&ifFlags == ifFlags {
			matched = true
		}
		if checkAttrs {
			if ip := ToIPAddr(ifAddr.SockAddr); ip != nil {
				netIP := (*ip).NetIP()
				switch {
				case wantGlobalUnicast && netIP.IsGlobalUnicast():
					matched = true
				case wantInterfaceLocalMulticast && netIP.IsInterfaceLocalMulticast():
					matched = true
				case wantLinkLocalMulticast && netIP.IsLinkLocalMulticast():
					matched = true
				case wantLinkLocalUnicast && netIP.IsLinkLocalUnicast():
					matched = true
				case wantLoopback && netIP.IsLoopback():
					matched = true
				case wantMulticast && netIP.IsMulticast():
					matched = true
				case wantUnspecified && netIP.IsUnspecified():
					matched = true
				case wantForwardable && !IsRFC(ForwardingBlacklist, ifAddr.SockAddr):
					matched = true
				}
			}
		}
		if matched {
			matchedAddrs = append(matchedAddrs, ifAddr)
		} else {
			excludedAddrs = append(excludedAddrs, ifAddr)
		}
	}
	return matchedAddrs, excludedAddrs, nil
}

func IfByNetwork(selectorParam string, inputIfAddrs IfAddrs) (IfAddrs, IfAddrs, error) {
	var includedIfs, excludedIfs IfAddrs
	for _, netStr := range strings.Split(selectorParam, "|") {
		netAddr, err := NewIPAddr(netStr)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to create an IP address from %+q: %v", netStr, err)
		}

		for _, ifAddr := range inputIfAddrs {
			if netAddr.Contains(ifAddr.SockAddr) {
				includedIfs = append(includedIfs, ifAddr)
			} else {
				excludedIfs = append(excludedIfs, ifAddr)
			}
		}
	}

	return includedIfs, excludedIfs, nil
}

func IfAddrMath(operation, value string, inputIfAddr IfAddr) (IfAddr, error) {

	signRe := signRE.Copy()

	switch strings.ToLower(operation) {
	case "address":

		if !signRe.MatchString(value) {
			return IfAddr{}, fmt.Errorf("sign (+/-) is required for operation %q", operation)
		}

		switch sockType := inputIfAddr.SockAddr.Type(); sockType {
		case TypeIPv4:

			i, err := strconv.ParseInt(value, 10, 33)
			if err != nil {
				return IfAddr{}, fmt.Errorf("unable to convert %q to int for operation %q: %v", value, operation, err)
			}

			ipv4 := *ToIPv4Addr(inputIfAddr.SockAddr)
			ipv4Uint32 := uint32(ipv4.Address)
			ipv4Uint32 += uint32(i)
			return IfAddr{
				SockAddr: IPv4Addr{
					Address: IPv4Address(ipv4Uint32),
					Mask:    ipv4.Mask,
				},
				Interface: inputIfAddr.Interface,
			}, nil
		case TypeIPv6:

			i, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return IfAddr{}, fmt.Errorf("unable to convert %q to int for operation %q: %v", value, operation, err)
			}

			ipv6 := *ToIPv6Addr(inputIfAddr.SockAddr)
			ipv6BigIntA := new(big.Int)
			ipv6BigIntA.Set(ipv6.Address)
			ipv6BigIntB := big.NewInt(i)

			ipv6Addr := ipv6BigIntA.Add(ipv6BigIntA, ipv6BigIntB)
			ipv6Addr.And(ipv6Addr, ipv6HostMask)

			return IfAddr{
				SockAddr: IPv6Addr{
					Address: IPv6Address(ipv6Addr),
					Mask:    ipv6.Mask,
				},
				Interface: inputIfAddr.Interface,
			}, nil
		default:
			return IfAddr{}, fmt.Errorf("unsupported type for operation %q: %T", operation, sockType)
		}
	case "network":

		if !signRe.MatchString(value) {
			return IfAddr{}, fmt.Errorf("sign (+/-) is required for operation %q", operation)
		}

		switch sockType := inputIfAddr.SockAddr.Type(); sockType {
		case TypeIPv4:

			i, err := strconv.ParseInt(value, 10, 33)
			if err != nil {
				return IfAddr{}, fmt.Errorf("unable to convert %q to int for operation %q: %v", value, operation, err)
			}

			ipv4 := *ToIPv4Addr(inputIfAddr.SockAddr)
			ipv4Uint32 := uint32(ipv4.NetworkAddress())

			var wrappedMask int64
			if i >= 0 {
				wrappedMask = i
			} else {
				wrappedMask = 1 + i + int64(^uint32(ipv4.Mask))
			}

			ipv4Uint32 = ipv4Uint32 + (uint32(wrappedMask) &^ uint32(ipv4.Mask))

			return IfAddr{
				SockAddr: IPv4Addr{
					Address: IPv4Address(ipv4Uint32),
					Mask:    ipv4.Mask,
				},
				Interface: inputIfAddr.Interface,
			}, nil
		case TypeIPv6:

			i, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return IfAddr{}, fmt.Errorf("unable to convert %q to int for operation %q: %v", value, operation, err)
			}

			ipv6 := *ToIPv6Addr(inputIfAddr.SockAddr)
			ipv6BigInt := new(big.Int)
			ipv6BigInt.Set(ipv6.NetworkAddress())

			mask := new(big.Int)
			mask.Set(ipv6.Mask)
			if i > 0 {
				wrappedMask := new(big.Int)
				wrappedMask.SetInt64(i)

				wrappedMask.AndNot(wrappedMask, mask)
				ipv6BigInt.Add(ipv6BigInt, wrappedMask)
			} else {

				wrappedMask := new(big.Int)
				wrappedMask.SetInt64(-1 * i)
				wrappedMask.Sub(wrappedMask, big.NewInt(1))

				wrappedMask.AndNot(wrappedMask, mask)

				lastUsable := new(big.Int)
				lastUsable.Set(ipv6.LastUsable().(IPv6Addr).Address)

				ipv6BigInt = lastUsable.Sub(lastUsable, wrappedMask)
			}

			return IfAddr{
				SockAddr: IPv6Addr{
					Address: IPv6Address(ipv6BigInt),
					Mask:    ipv6.Mask,
				},
				Interface: inputIfAddr.Interface,
			}, nil
		default:
			return IfAddr{}, fmt.Errorf("unsupported type for operation %q: %T", operation, sockType)
		}
	case "mask":

		switch sockType := inputIfAddr.SockAddr.Type(); sockType {
		case TypeIPv4:
			i, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return IfAddr{}, fmt.Errorf("unable to convert %q to int for operation %q: %v", value, operation, err)
			}

			if i > 32 {
				return IfAddr{}, fmt.Errorf("parameter for operation %q on ipv4 addresses must be between 0 and 32", operation)
			}

			ipv4 := *ToIPv4Addr(inputIfAddr.SockAddr)

			ipv4Mask := net.CIDRMask(int(i), 32)
			ipv4MaskUint32 := binary.BigEndian.Uint32(ipv4Mask)

			maskedIpv4 := ipv4.NetIP().Mask(ipv4Mask)
			maskedIpv4Uint32 := binary.BigEndian.Uint32(maskedIpv4)

			maskedIpv4MaskUint32 := uint32(ipv4.Mask)

			if ipv4MaskUint32 < maskedIpv4MaskUint32 {
				maskedIpv4MaskUint32 = ipv4MaskUint32
			}

			return IfAddr{
				SockAddr: IPv4Addr{
					Address: IPv4Address(maskedIpv4Uint32),
					Mask:    IPv4Mask(maskedIpv4MaskUint32),
				},
				Interface: inputIfAddr.Interface,
			}, nil
		case TypeIPv6:
			i, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return IfAddr{}, fmt.Errorf("unable to convert %q to int for operation %q: %v", value, operation, err)
			}

			if i > 128 {
				return IfAddr{}, fmt.Errorf("parameter for operation %q on ipv6 addresses must be between 0 and 64", operation)
			}

			ipv6 := *ToIPv6Addr(inputIfAddr.SockAddr)

			ipv6Mask := net.CIDRMask(int(i), 128)
			ipv6MaskBigInt := new(big.Int)
			ipv6MaskBigInt.SetBytes(ipv6Mask)

			maskedIpv6 := ipv6.NetIP().Mask(ipv6Mask)
			maskedIpv6BigInt := new(big.Int)
			maskedIpv6BigInt.SetBytes(maskedIpv6)

			maskedIpv6MaskBigInt := new(big.Int)
			maskedIpv6MaskBigInt.Set(ipv6.Mask)

			if ipv6MaskBigInt.Cmp(maskedIpv6MaskBigInt) == -1 {
				maskedIpv6MaskBigInt = ipv6MaskBigInt
			}

			return IfAddr{
				SockAddr: IPv6Addr{
					Address: IPv6Address(maskedIpv6BigInt),
					Mask:    IPv6Mask(maskedIpv6MaskBigInt),
				},
				Interface: inputIfAddr.Interface,
			}, nil
		default:
			return IfAddr{}, fmt.Errorf("unsupported type for operation %q: %T", operation, sockType)
		}
	default:
		return IfAddr{}, fmt.Errorf("unsupported math operation: %q", operation)
	}
}

func IfAddrsMath(operation, value string, inputIfAddrs IfAddrs) (IfAddrs, error) {
	outputAddrs := make(IfAddrs, 0, len(inputIfAddrs))
	for _, ifAddr := range inputIfAddrs {
		result, err := IfAddrMath(operation, value, ifAddr)
		if err != nil {
			return IfAddrs{}, fmt.Errorf("unable to perform an IPMath operation on %s: %v", ifAddr, err)
		}
		outputAddrs = append(outputAddrs, result)
	}
	return outputAddrs, nil
}

func IncludeIfs(selectorName, selectorParam string, inputIfAddrs IfAddrs) (IfAddrs, error) {
	var includedIfs IfAddrs
	var err error

	switch strings.ToLower(selectorName) {
	case "address":
		includedIfs, _, err = IfByAddress(selectorParam, inputIfAddrs)
	case "flag", "flags":
		includedIfs, _, err = IfByFlag(selectorParam, inputIfAddrs)
	case "name":
		includedIfs, _, err = IfByName(selectorParam, inputIfAddrs)
	case "network":
		includedIfs, _, err = IfByNetwork(selectorParam, inputIfAddrs)
	case "port":
		includedIfs, _, err = IfByPort(selectorParam, inputIfAddrs)
	case "rfc", "rfcs":
		includedIfs, _, err = IfByRFCs(selectorParam, inputIfAddrs)
	case "size":
		includedIfs, _, err = IfByMaskSize(selectorParam, inputIfAddrs)
	case "type":
		includedIfs, _, err = IfByType(selectorParam, inputIfAddrs)
	default:
		return IfAddrs{}, fmt.Errorf("invalid include selector %q", selectorName)
	}

	if err != nil {
		return IfAddrs{}, err
	}

	return includedIfs, nil
}

func ExcludeIfs(selectorName, selectorParam string, inputIfAddrs IfAddrs) (IfAddrs, error) {
	var excludedIfs IfAddrs
	var err error

	switch strings.ToLower(selectorName) {
	case "address":
		_, excludedIfs, err = IfByAddress(selectorParam, inputIfAddrs)
	case "flag", "flags":
		_, excludedIfs, err = IfByFlag(selectorParam, inputIfAddrs)
	case "name":
		_, excludedIfs, err = IfByName(selectorParam, inputIfAddrs)
	case "network":
		_, excludedIfs, err = IfByNetwork(selectorParam, inputIfAddrs)
	case "port":
		_, excludedIfs, err = IfByPort(selectorParam, inputIfAddrs)
	case "rfc", "rfcs":
		_, excludedIfs, err = IfByRFCs(selectorParam, inputIfAddrs)
	case "size":
		_, excludedIfs, err = IfByMaskSize(selectorParam, inputIfAddrs)
	case "type":
		_, excludedIfs, err = IfByType(selectorParam, inputIfAddrs)
	default:
		return IfAddrs{}, fmt.Errorf("invalid exclude selector %q", selectorName)
	}

	if err != nil {
		return IfAddrs{}, err
	}

	return excludedIfs, nil
}

func SortIfBy(selectorParam string, inputIfAddrs IfAddrs) (IfAddrs, error) {
	sortedIfs := append(IfAddrs(nil), inputIfAddrs...)

	clauses := strings.Split(selectorParam, ",")
	sortFuncs := make([]CmpIfAddrFunc, len(clauses))

	for i, clause := range clauses {
		switch strings.TrimSpace(strings.ToLower(clause)) {
		case "+address", "address":

			sortFuncs[i] = AscIfAddress
		case "-address":
			sortFuncs[i] = DescIfAddress
		case "+default", "default":
			sortFuncs[i] = AscIfDefault
		case "-default":
			sortFuncs[i] = DescIfDefault
		case "+name", "name":

			sortFuncs[i] = AscIfName
		case "-name":
			sortFuncs[i] = DescIfName
		case "+port", "port":

			sortFuncs[i] = AscIfPort
		case "-port":
			sortFuncs[i] = DescIfPort
		case "+private", "private":

			sortFuncs[i] = AscIfPrivate
		case "-private":
			sortFuncs[i] = DescIfPrivate
		case "+size", "size":

			sortFuncs[i] = AscIfNetworkSize
		case "-size":
			sortFuncs[i] = DescIfNetworkSize
		case "+type", "type":

			sortFuncs[i] = AscIfType
		case "-type":
			sortFuncs[i] = DescIfType
		default:

			return IfAddrs{}, fmt.Errorf("unknown sort type: %q", clause)
		}
	}

	OrderedIfAddrBy(sortFuncs...).Sort(sortedIfs)

	return sortedIfs, nil
}

func UniqueIfAddrsBy(selectorName string, inputIfAddrs IfAddrs) (IfAddrs, error) {
	attrName := strings.ToLower(selectorName)

	ifs := make(IfAddrs, 0, len(inputIfAddrs))
	var lastMatch string
	for _, ifAddr := range inputIfAddrs {
		var out string
		switch attrName {
		case "address":
			out = ifAddr.SockAddr.String()
		case "name":
			out = ifAddr.Name
		default:
			return nil, fmt.Errorf("unsupported unique constraint %+q", selectorName)
		}

		switch {
		case lastMatch == "", lastMatch != out:
			lastMatch = out
			ifs = append(ifs, ifAddr)
		case lastMatch == out:
			continue
		}
	}

	return ifs, nil
}

func JoinIfAddrs(selectorName string, joinStr string, inputIfAddrs IfAddrs) (string, error) {
	outputs := make([]string, 0, len(inputIfAddrs))
	attrName := AttrName(strings.ToLower(selectorName))

	for _, ifAddr := range inputIfAddrs {
		var attrVal string
		var err error
		attrVal, err = ifAddr.Attr(attrName)
		if err != nil {
			return "", err
		}
		outputs = append(outputs, attrVal)
	}
	return strings.Join(outputs, joinStr), nil
}

func LimitIfAddrs(lim uint, in IfAddrs) (IfAddrs, error) {

	if int(lim) > len(in) {
		lim = uint(len(in))
	}

	return in[0:lim], nil
}

func OffsetIfAddrs(off int, in IfAddrs) (IfAddrs, error) {
	var end bool
	if off < 0 {
		end = true
		off = off * -1
	}

	if off > len(in) {
		return IfAddrs{}, fmt.Errorf("unable to seek past the end of the interface array: offset (%d) exceeds the number of interfaces (%d)", off, len(in))
	}

	if end {
		return in[len(in)-off:], nil
	}
	return in[off:], nil
}

func (ifAddr IfAddr) String() string {
	return fmt.Sprintf("%s %v", ifAddr.SockAddr, ifAddr.Interface)
}

func parseDefaultIfNameFromRoute(routeOut string) (string, error) {
	lines := strings.Split(routeOut, "\n")
	for _, line := range lines {
		kvs := strings.SplitN(line, ":", 2)
		if len(kvs) != 2 {
			continue
		}

		if strings.TrimSpace(kvs[0]) == "interface" {
			ifName := strings.TrimSpace(kvs[1])
			return ifName, nil
		}
	}

	return "", errors.New("No default interface found")
}

func parseDefaultIfNameFromIPCmd(routeOut string) (string, error) {
	lines := strings.Split(routeOut, "\n")
	re := whitespaceRE.Copy()
	for _, line := range lines {
		kvs := re.Split(line, -1)
		if len(kvs) < 5 {
			continue
		}

		if kvs[0] == "default" &&
			kvs[1] == "via" &&
			kvs[3] == "dev" {
			ifName := strings.TrimSpace(kvs[4])
			return ifName, nil
		}
	}

	return "", errors.New("No default interface found")
}

func parseDefaultIfNameWindows(routeOut, ipconfigOut string) (string, error) {
	defaultIPAddr, err := parseDefaultIPAddrWindowsRoute(routeOut)
	if err != nil {
		return "", err
	}

	ifName, err := parseDefaultIfNameWindowsIPConfig(defaultIPAddr, ipconfigOut)
	if err != nil {
		return "", err
	}

	return ifName, nil
}

//
func parseDefaultIPAddrWindowsRoute(routeOut string) (string, error) {
	lines := strings.Split(routeOut, "\n")
	re := whitespaceRE.Copy()
	for _, line := range lines {
		kvs := re.Split(strings.TrimSpace(line), -1)
		if len(kvs) < 3 {
			continue
		}

		if kvs[0] == "0.0.0.0" && kvs[1] == "0.0.0.0" {
			defaultIPAddr := strings.TrimSpace(kvs[3])
			return defaultIPAddr, nil
		}
	}

	return "", errors.New("No IP on default interface found")
}

func parseDefaultIfNameWindowsIPConfig(defaultIPAddr, routeOut string) (string, error) {
	lines := strings.Split(routeOut, "\n")
	ifNameRe := ifNameRE.Copy()
	ipAddrRe := ipAddrRE.Copy()
	var ifName string
	for _, line := range lines {
		switch ifNameMatches := ifNameRe.FindStringSubmatch(line); {
		case len(ifNameMatches) > 1:
			ifName = ifNameMatches[1]
			continue
		}

		switch ipAddrMatches := ipAddrRe.FindStringSubmatch(line); {
		case len(ipAddrMatches) > 1 && ipAddrMatches[1] == defaultIPAddr:
			return ifName, nil
		}
	}

	return "", errors.New("No default interface found with matching IP")
}
