// This file is taken from the log/syslog in the standard lib.
//
// +build !windows,!nacl,!plan9

package gsyslog

import (
	"errors"
	"fmt"
	"log/syslog"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const severityMask = 0x07
const facilityMask = 0xf8
const localDeadline = 20 * time.Millisecond
const remoteDeadline = 50 * time.Millisecond

type builtinWriter struct {
	priority syslog.Priority
	tag      string
	hostname string
	network  string
	raddr    string

	mu   sync.Mutex // guards conn
	conn serverConn
}

type serverConn interface {
	writeString(p syslog.Priority, hostname, tag, s, nl string) error
	close() error
}

type netConn struct {
	local bool
	conn  net.Conn
}

func newBuiltin(priority syslog.Priority, tag string) (w *builtinWriter, err error) {
	return dialBuiltin("", "", priority, tag)
}

func dialBuiltin(network, raddr string, priority syslog.Priority, tag string) (*builtinWriter, error) {
	if priority < 0 || priority > syslog.LOG_LOCAL7|syslog.LOG_DEBUG {
		return nil, errors.New("log/syslog: invalid priority")
	}

	if tag == "" {
		tag = os.Args[0]
	}
	hostname, _ := os.Hostname()

	w := &builtinWriter{
		priority: priority,
		tag:      tag,
		hostname: hostname,
		network:  network,
		raddr:    raddr,
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	err := w.connect()
	if err != nil {
		return nil, err
	}
	return w, err
}

func (w *builtinWriter) connect() (err error) {
	if w.conn != nil {

		w.conn.close()
		w.conn = nil
	}

	if w.network == "" {
		w.conn, err = unixSyslog()
		if w.hostname == "" {
			w.hostname = "localhost"
		}
	} else {
		var c net.Conn
		c, err = net.DialTimeout(w.network, w.raddr, remoteDeadline)
		if err == nil {
			w.conn = &netConn{conn: c}
			if w.hostname == "" {
				w.hostname = c.LocalAddr().String()
			}
		}
	}
	return
}

func (w *builtinWriter) Write(b []byte) (int, error) {
	return w.writeAndRetry(w.priority, string(b))
}

func (w *builtinWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.conn != nil {
		err := w.conn.close()
		w.conn = nil
		return err
	}
	return nil
}

func (w *builtinWriter) writeAndRetry(p syslog.Priority, s string) (int, error) {
	pr := (w.priority & facilityMask) | (p & severityMask)

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.conn != nil {
		if n, err := w.write(pr, s); err == nil {
			return n, err
		}
	}
	if err := w.connect(); err != nil {
		return 0, err
	}
	return w.write(pr, s)
}

func (w *builtinWriter) write(p syslog.Priority, msg string) (int, error) {

	nl := ""
	if !strings.HasSuffix(msg, "\n") {
		nl = "\n"
	}

	err := w.conn.writeString(p, w.hostname, w.tag, msg, nl)
	if err != nil {
		return 0, err
	}

	return len(msg), nil
}

func (n *netConn) writeString(p syslog.Priority, hostname, tag, msg, nl string) error {
	if n.local {

		timestamp := time.Now().Format(time.Stamp)
		n.conn.SetWriteDeadline(time.Now().Add(localDeadline))
		_, err := fmt.Fprintf(n.conn, "<%d>%s %s[%d]: %s%s",
			p, timestamp,
			tag, os.Getpid(), msg, nl)
		return err
	}
	timestamp := time.Now().Format(time.RFC3339)
	n.conn.SetWriteDeadline(time.Now().Add(remoteDeadline))
	_, err := fmt.Fprintf(n.conn, "<%d>%s %s %s[%d]: %s%s",
		p, timestamp, hostname,
		tag, os.Getpid(), msg, nl)
	return err
}

func (n *netConn) close() error {
	return n.conn.Close()
}

func unixSyslog() (conn serverConn, err error) {
	logTypes := []string{"unixgram", "unix"}
	logPaths := []string{"/dev/log", "/var/run/syslog"}
	for _, network := range logTypes {
		for _, path := range logPaths {
			conn, err := net.DialTimeout(network, path, localDeadline)
			if err != nil {
				continue
			} else {
				return &netConn{conn: conn, local: true}, nil
			}
		}
	}
	return nil, errors.New("Unix syslog delivery error")
}
