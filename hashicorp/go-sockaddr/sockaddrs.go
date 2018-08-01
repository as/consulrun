package sockaddr

import (
	"bytes"
	"sort"
)

type SockAddrs []SockAddr

func (s SockAddrs) Len() int      { return len(s) }
func (s SockAddrs) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

type CmpAddrFunc func(p1, p2 *SockAddr) int

type multiAddrSorter struct {
	addrs SockAddrs
	cmp   []CmpAddrFunc
}

func (ms *multiAddrSorter) Sort(sockAddrs SockAddrs) {
	ms.addrs = sockAddrs
	sort.Sort(ms)
}

func OrderedAddrBy(cmpFuncs ...CmpAddrFunc) *multiAddrSorter {
	return &multiAddrSorter{
		cmp: cmpFuncs,
	}
}

func (ms *multiAddrSorter) Len() int {
	return len(ms.addrs)
}

func (ms *multiAddrSorter) Less(i, j int) bool {
	p, q := &ms.addrs[i], &ms.addrs[j]

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
	}
}

func (ms *multiAddrSorter) Swap(i, j int) {
	ms.addrs[i], ms.addrs[j] = ms.addrs[j], ms.addrs[i]
}

const (
	sortReceiverBeforeArg = -1
	sortDeferDecision     = 0
	sortArgBeforeReceiver = 1
)

func AscAddress(p1Ptr, p2Ptr *SockAddr) int {
	p1 := *p1Ptr
	p2 := *p2Ptr

	switch v := p1.(type) {
	case IPv4Addr:
		return v.CmpAddress(p2)
	case IPv6Addr:
		return v.CmpAddress(p2)
	case UnixSock:
		return v.CmpAddress(p2)
	default:
		return sortDeferDecision
	}
}

func AscPort(p1Ptr, p2Ptr *SockAddr) int {
	p1 := *p1Ptr
	p2 := *p2Ptr

	switch v := p1.(type) {
	case IPv4Addr:
		return v.CmpPort(p2)
	case IPv6Addr:
		return v.CmpPort(p2)
	default:
		return sortDeferDecision
	}
}

func AscPrivate(p1Ptr, p2Ptr *SockAddr) int {
	p1 := *p1Ptr
	p2 := *p2Ptr

	switch v := p1.(type) {
	case IPv4Addr, IPv6Addr:
		return v.CmpRFC(6890, p2)
	default:
		return sortDeferDecision
	}
}

func AscNetworkSize(p1Ptr, p2Ptr *SockAddr) int {
	p1 := *p1Ptr
	p2 := *p2Ptr
	p1Type := p1.Type()
	p2Type := p2.Type()

	if p1Type != p2Type && p1Type != TypeIP {
		return sortDeferDecision
	}

	ipA := p1.(IPAddr)
	ipB := p2.(IPAddr)

	return bytes.Compare([]byte(*ipA.NetIPMask()), []byte(*ipB.NetIPMask()))
}

func AscType(p1Ptr, p2Ptr *SockAddr) int {
	p1 := *p1Ptr
	p2 := *p2Ptr
	p1Type := p1.Type()
	p2Type := p2.Type()
	switch {
	case p1Type < p2Type:
		return sortReceiverBeforeArg
	case p1Type == p2Type:
		return sortDeferDecision
	case p1Type > p2Type:
		return sortArgBeforeReceiver
	default:
		return sortDeferDecision
	}
}

func (sas SockAddrs) FilterByType(type_ SockAddrType) (matched, excluded SockAddrs) {
	matched = make(SockAddrs, 0, len(sas))
	excluded = make(SockAddrs, 0, len(sas))

	for _, sa := range sas {
		if sa.Type()&type_ != 0 {
			matched = append(matched, sa)
		} else {
			excluded = append(excluded, sa)
		}
	}
	return matched, excluded
}
