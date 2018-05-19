// +build windows

package dns

import "net"

type SessionUDP struct {
	raddr *net.UDPAddr
}

func (s *SessionUDP) RemoteAddr() net.Addr { return s.raddr }

func ReadFromSessionUDP(conn *net.UDPConn, b []byte) (int, *SessionUDP, error) {
	n, raddr, err := conn.ReadFrom(b)
	if err != nil {
		return n, nil, err
	}
	session := &SessionUDP{raddr.(*net.UDPAddr)}
	return n, session, err
}

func WriteToSessionUDP(conn *net.UDPConn, b []byte, session *SessionUDP) (int, error) {
	n, err := conn.WriteTo(b, session.raddr)
	return n, err
}

func setUDPSocketOptions(*net.UDPConn) error { return nil }
func parseDstFromOOB([]byte, net.IP) net.IP  { return nil }
