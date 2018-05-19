package sockaddr

import (
	"encoding/binary"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

type (
	IPv4Address uint32

	IPv4Network uint32

	IPv4Mask uint32
)

const IPv4HostMask = IPv4Mask(0xffffffff)

var ipv4AddrAttrMap map[AttrName]func(IPv4Addr) string
var ipv4AddrAttrs []AttrName
var trailingHexNetmaskRE *regexp.Regexp

type IPv4Addr struct {
	IPAddr
	Address IPv4Address
	Mask    IPv4Mask
	Port    IPPort
}

func init() {
	ipv4AddrInit()
	trailingHexNetmaskRE = regexp.MustCompile(`/([0f]{8})$`)
}

//
func NewIPv4Addr(ipv4Str string) (IPv4Addr, error) {

	trailingHexNetmaskRe := trailingHexNetmaskRE.Copy()
	if match := trailingHexNetmaskRe.FindStringIndex(ipv4Str); match != nil {
		ipv4Str = ipv4Str[:match[0]]
	}

	ipAddr, network, err := net.ParseCIDR(ipv4Str)
	if err == nil {
		ipv4 := ipAddr.To4()
		if ipv4 == nil {
			return IPv4Addr{}, fmt.Errorf("Unable to convert %s to an IPv4 address", ipv4Str)
		}

		netmaskSepPos := strings.LastIndexByte(ipv4Str, '/')
		if netmaskSepPos != -1 && netmaskSepPos+1 < len(ipv4Str) {
			netMask, err := strconv.ParseUint(ipv4Str[netmaskSepPos+1:], 10, 8)
			if err != nil {
				return IPv4Addr{}, fmt.Errorf("Unable to convert %s to an IPv4 address: unable to parse CIDR netmask: %v", ipv4Str, err)
			} else if netMask > 128 {
				return IPv4Addr{}, fmt.Errorf("Unable to convert %s to an IPv4 address: invalid CIDR netmask", ipv4Str)
			}

			if netMask >= 96 {

				network.Mask = net.CIDRMask(int(netMask-96), IPv4len*8)
			}
		}
		ipv4Addr := IPv4Addr{
			Address: IPv4Address(binary.BigEndian.Uint32(ipv4)),
			Mask:    IPv4Mask(binary.BigEndian.Uint32(network.Mask)),
		}
		return ipv4Addr, nil
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp4", ipv4Str)
	if err == nil {
		ipv4 := tcpAddr.IP.To4()
		if ipv4 == nil {
			return IPv4Addr{}, fmt.Errorf("Unable to resolve %+q as an IPv4 address", ipv4Str)
		}

		ipv4Uint32 := binary.BigEndian.Uint32(ipv4)
		ipv4Addr := IPv4Addr{
			Address: IPv4Address(ipv4Uint32),
			Mask:    IPv4HostMask,
			Port:    IPPort(tcpAddr.Port),
		}

		return ipv4Addr, nil
	}

	ip := net.ParseIP(ipv4Str)
	if ip != nil {
		ipv4 := ip.To4()
		if ipv4 == nil {
			return IPv4Addr{}, fmt.Errorf("Unable to string convert %+q to an IPv4 address", ipv4Str)
		}

		ipv4Uint32 := binary.BigEndian.Uint32(ipv4)
		ipv4Addr := IPv4Addr{
			Address: IPv4Address(ipv4Uint32),
			Mask:    IPv4HostMask,
		}
		return ipv4Addr, nil
	}

	return IPv4Addr{}, fmt.Errorf("Unable to parse %+q to an IPv4 address: %v", ipv4Str, err)
}

func (ipv4 IPv4Addr) AddressBinString() string {
	return fmt.Sprintf("%032s", strconv.FormatUint(uint64(ipv4.Address), 2))
}

func (ipv4 IPv4Addr) AddressHexString() string {
	return fmt.Sprintf("%08s", strconv.FormatUint(uint64(ipv4.Address), 16))
}

//
func (ipv4 IPv4Addr) Broadcast() IPAddr {

	return IPv4Addr{
		Address: IPv4Address(ipv4.BroadcastAddress()),
		Mask:    IPv4HostMask,
	}
}

func (ipv4 IPv4Addr) BroadcastAddress() IPv4Network {
	return IPv4Network(uint32(ipv4.Address)&uint32(ipv4.Mask) | ^uint32(ipv4.Mask))
}

//
func (ipv4 IPv4Addr) CmpAddress(sa SockAddr) int {
	ipv4b, ok := sa.(IPv4Addr)
	if !ok {
		return sortDeferDecision
	}

	switch {
	case ipv4.Address == ipv4b.Address:
		return sortDeferDecision
	case ipv4.Address < ipv4b.Address:
		return sortReceiverBeforeArg
	default:
		return sortArgBeforeReceiver
	}
}

//
func (ipv4 IPv4Addr) CmpPort(sa SockAddr) int {
	var saPort IPPort
	switch v := sa.(type) {
	case IPv4Addr:
		saPort = v.Port
	case IPv6Addr:
		saPort = v.Port
	default:
		return sortDeferDecision
	}

	switch {
	case ipv4.Port == saPort:
		return sortDeferDecision
	case ipv4.Port < saPort:
		return sortReceiverBeforeArg
	default:
		return sortArgBeforeReceiver
	}
}

//
func (ipv4 IPv4Addr) CmpRFC(rfcNum uint, sa SockAddr) int {
	recvInRFC := IsRFC(rfcNum, ipv4)
	ipv4b, ok := sa.(IPv4Addr)
	if !ok {

		if recvInRFC {
			return sortReceiverBeforeArg
		} else {
			return sortDeferDecision
		}
	}

	argInRFC := IsRFC(rfcNum, ipv4b)
	switch {
	case (recvInRFC && argInRFC), (!recvInRFC && !argInRFC):

		return sortDeferDecision
	case recvInRFC && !argInRFC:
		return sortReceiverBeforeArg
	default:
		return sortArgBeforeReceiver
	}
}

func (ipv4 IPv4Addr) Contains(sa SockAddr) bool {
	ipv4b, ok := sa.(IPv4Addr)
	if !ok {
		return false
	}

	return ipv4.ContainsNetwork(ipv4b)
}

func (ipv4 IPv4Addr) ContainsAddress(x IPv4Address) bool {
	return IPv4Address(ipv4.NetworkAddress()) <= x &&
		IPv4Address(ipv4.BroadcastAddress()) >= x
}

func (ipv4 IPv4Addr) ContainsNetwork(x IPv4Addr) bool {
	return ipv4.NetworkAddress() <= x.NetworkAddress() &&
		ipv4.BroadcastAddress() >= x.BroadcastAddress()
}

func (ipv4 IPv4Addr) DialPacketArgs() (network, dialArgs string) {
	if ipv4.Mask != IPv4HostMask || ipv4.Port == 0 {
		return "udp4", ""
	}
	return "udp4", fmt.Sprintf("%s:%d", ipv4.NetIP().String(), ipv4.Port)
}

func (ipv4 IPv4Addr) DialStreamArgs() (network, dialArgs string) {
	if ipv4.Mask != IPv4HostMask || ipv4.Port == 0 {
		return "tcp4", ""
	}
	return "tcp4", fmt.Sprintf("%s:%d", ipv4.NetIP().String(), ipv4.Port)
}

func (ipv4 IPv4Addr) Equal(sa SockAddr) bool {
	ipv4b, ok := sa.(IPv4Addr)
	if !ok {
		return false
	}

	if ipv4.Port != ipv4b.Port {
		return false
	}

	if ipv4.Address != ipv4b.Address {
		return false
	}

	if ipv4.NetIPNet().String() != ipv4b.NetIPNet().String() {
		return false
	}

	return true
}

func (ipv4 IPv4Addr) FirstUsable() IPAddr {
	addr := ipv4.NetworkAddress()

	if ipv4.Maskbits() < 31 {
		addr++
	}

	return IPv4Addr{
		Address: IPv4Address(addr),
		Mask:    IPv4HostMask,
	}
}

func (ipv4 IPv4Addr) Host() IPAddr {

	return IPv4Addr{
		Address: ipv4.Address,
		Mask:    IPv4HostMask,
		Port:    ipv4.Port,
	}
}

func (ipv4 IPv4Addr) IPPort() IPPort {
	return ipv4.Port
}

func (ipv4 IPv4Addr) LastUsable() IPAddr {
	addr := ipv4.BroadcastAddress()

	if ipv4.Maskbits() < 31 {
		addr--
	}

	return IPv4Addr{
		Address: IPv4Address(addr),
		Mask:    IPv4HostMask,
	}
}

func (ipv4 IPv4Addr) ListenPacketArgs() (network, listenArgs string) {
	if ipv4.Mask != IPv4HostMask {
		return "udp4", ""
	}
	return "udp4", fmt.Sprintf("%s:%d", ipv4.NetIP().String(), ipv4.Port)
}

func (ipv4 IPv4Addr) ListenStreamArgs() (network, listenArgs string) {
	if ipv4.Mask != IPv4HostMask {
		return "tcp4", ""
	}
	return "tcp4", fmt.Sprintf("%s:%d", ipv4.NetIP().String(), ipv4.Port)
}

func (ipv4 IPv4Addr) Maskbits() int {
	mask := make(net.IPMask, IPv4len)
	binary.BigEndian.PutUint32(mask, uint32(ipv4.Mask))
	maskOnes, _ := mask.Size()
	return maskOnes
}

func MustIPv4Addr(addr string) IPv4Addr {
	ipv4, err := NewIPv4Addr(addr)
	if err != nil {
		panic(fmt.Sprintf("Unable to create an IPv4Addr from %+q: %v", addr, err))
	}
	return ipv4
}

func (ipv4 IPv4Addr) NetIP() *net.IP {
	x := make(net.IP, IPv4len)
	binary.BigEndian.PutUint32(x, uint32(ipv4.Address))
	return &x
}

func (ipv4 IPv4Addr) NetIPMask() *net.IPMask {
	ipv4Mask := net.IPMask{}
	ipv4Mask = make(net.IPMask, IPv4len)
	binary.BigEndian.PutUint32(ipv4Mask, uint32(ipv4.Mask))
	return &ipv4Mask
}

func (ipv4 IPv4Addr) NetIPNet() *net.IPNet {
	ipv4net := &net.IPNet{}
	ipv4net.IP = make(net.IP, IPv4len)
	binary.BigEndian.PutUint32(ipv4net.IP, uint32(ipv4.NetworkAddress()))
	ipv4net.Mask = *ipv4.NetIPMask()
	return ipv4net
}

func (ipv4 IPv4Addr) Network() IPAddr {
	return IPv4Addr{
		Address: IPv4Address(ipv4.NetworkAddress()),
		Mask:    ipv4.Mask,
	}
}

func (ipv4 IPv4Addr) NetworkAddress() IPv4Network {
	return IPv4Network(uint32(ipv4.Address) & uint32(ipv4.Mask))
}

func (ipv4 IPv4Addr) Octets() []int {
	return []int{
		int(ipv4.Address >> 24),
		int((ipv4.Address >> 16) & 0xff),
		int((ipv4.Address >> 8) & 0xff),
		int(ipv4.Address & 0xff),
	}
}

func (ipv4 IPv4Addr) String() string {
	if ipv4.Port != 0 {
		return fmt.Sprintf("%s:%d", ipv4.NetIP().String(), ipv4.Port)
	}

	if ipv4.Maskbits() == 32 {
		return ipv4.NetIP().String()
	}

	return fmt.Sprintf("%s/%d", ipv4.NetIP().String(), ipv4.Maskbits())
}

func (IPv4Addr) Type() SockAddrType {
	return TypeIPv4
}

func IPv4AddrAttr(ipv4 IPv4Addr, selector AttrName) string {
	fn, found := ipv4AddrAttrMap[selector]
	if !found {
		return ""
	}

	return fn(ipv4)
}

func IPv4Attrs() []AttrName {
	return ipv4AddrAttrs
}

func ipv4AddrInit() {

	ipv4AddrAttrs = []AttrName{
		"size", // Same position as in IPv6 for output consistency
		"broadcast",
		"uint32",
	}

	ipv4AddrAttrMap = map[AttrName]func(ipv4 IPv4Addr) string{
		"broadcast": func(ipv4 IPv4Addr) string {
			return ipv4.Broadcast().String()
		},
		"size": func(ipv4 IPv4Addr) string {
			return fmt.Sprintf("%d", 1<<uint(IPv4len*8-ipv4.Maskbits()))
		},
		"uint32": func(ipv4 IPv4Addr) string {
			return fmt.Sprintf("%d", uint32(ipv4.Address))
		},
	}
}
