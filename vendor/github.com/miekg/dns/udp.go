// +build !windows

package dns

import (
	"net"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

type SessionUDP struct {
	raddr   *net.UDPAddr
	context []byte
}

func (s *SessionUDP) RemoteAddr() net.Addr { return s.raddr }

func ReadFromSessionUDP(conn *net.UDPConn, b []byte) (int, *SessionUDP, error) {
	oob := make([]byte, 40)
	n, oobn, _, raddr, err := conn.ReadMsgUDP(b, oob)
	if err != nil {
		return n, nil, err
	}
	return n, &SessionUDP{raddr, oob[:oobn]}, err
}

func WriteToSessionUDP(conn *net.UDPConn, b []byte, session *SessionUDP) (int, error) {
	oob := correctSource(session.context)
	n, _, err := conn.WriteMsgUDP(b, oob, session.raddr)
	return n, err
}

func setUDPSocketOptions(conn *net.UDPConn) error {

	err6 := ipv6.NewPacketConn(conn).SetControlMessage(ipv6.FlagDst|ipv6.FlagInterface, true)
	err4 := ipv4.NewPacketConn(conn).SetControlMessage(ipv4.FlagDst|ipv4.FlagInterface, true)
	if err6 != nil && err4 != nil {
		return err4
	}
	return nil
}

func parseDstFromOOB(oob []byte) net.IP {

	var dst net.IP
	cm6 := new(ipv6.ControlMessage)
	if cm6.Parse(oob) == nil {
		dst = cm6.Dst
	}
	if dst == nil {
		cm4 := new(ipv4.ControlMessage)
		if cm4.Parse(oob) == nil {
			dst = cm4.Dst
		}
	}
	return dst
}

func correctSource(oob []byte) []byte {
	dst := parseDstFromOOB(oob)
	if dst == nil {
		return nil
	}

	if dst.To4() == nil {
		cm := new(ipv6.ControlMessage)
		cm.Src = dst
		oob = cm.Marshal()
	} else {
		cm := new(ipv4.ControlMessage)
		cm.Src = dst
		oob = cm.Marshal()
	}
	return oob
}
