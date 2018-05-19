package sockaddr

import "bytes"

type IPAddrs []IPAddr

func (s IPAddrs) Len() int      { return len(s) }
func (s IPAddrs) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

type SortIPAddrsByNetworkSize struct{ IPAddrs }

func (s SortIPAddrsByNetworkSize) Less(i, j int) bool {

	switch bytes.Compare([]byte(*s.IPAddrs[i].NetIPMask()), []byte(*s.IPAddrs[j].NetIPMask())) {
	case 0:

		break
	case 1:
		return true
	case -1:
		return false
	default:
		panic("bad, m'kay?")
	}

	iLen := len(*s.IPAddrs[i].NetIP())
	jLen := len(*s.IPAddrs[j].NetIP())
	if iLen != jLen {
		return iLen > jLen
	}

	switch bytes.Compare(s.IPAddrs[i].NetIPNet().IP, s.IPAddrs[j].NetIPNet().IP) {
	case 0:
		break
	case 1:
		return false
	case -1:
		return true
	default:
		panic("lol wut?")
	}

	if s.IPAddrs[i].IPPort() == 0 || s.IPAddrs[j].IPPort() == 0 {
		return false
	}
	return s.IPAddrs[i].IPPort() < s.IPAddrs[j].IPPort()
}

type SortIPAddrsBySpecificMaskLen struct{ IPAddrs }

func (s SortIPAddrsBySpecificMaskLen) Less(i, j int) bool {
	return s.IPAddrs[i].Maskbits() > s.IPAddrs[j].Maskbits()
}

type SortIPAddrsByBroadMaskLen struct{ IPAddrs }

func (s SortIPAddrsByBroadMaskLen) Less(i, j int) bool {
	return s.IPAddrs[i].Maskbits() < s.IPAddrs[j].Maskbits()
}
