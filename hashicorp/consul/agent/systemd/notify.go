package systemd

import (
	"errors"
	"net"
	"os"
)

const (
	Ready     = "READY=1"
	Reloading = "RELOADING=1"
	Stopping  = "STOPPING=1"
)

var NotifyNoSocket = errors.New("No socket")

type Notifier struct{}

func (n *Notifier) Notify(state string) error {
	addr := &net.UnixAddr{
		Name: os.Getenv("NOTIFY_SOCKET"),
		Net:  "unixgram",
	}

	if addr.Name == "" {
		return NotifyNoSocket
	}

	conn, err := net.DialUnix(addr.Net, nil, addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Write([]byte(state))
	return err
}
