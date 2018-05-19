package agent

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	osuser "os/user"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/consul/types"
	"github.com/hashicorp/go-msgpack/codec"
)

var msgpackHandle = &codec.MsgpackHandle{
	RawToString: true,
	WriteExt:    true,
}

func decodeMsgPack(buf []byte, out interface{}) error {
	return codec.NewDecoder(bytes.NewReader(buf), msgpackHandle).Decode(out)
}

func encodeMsgPack(msg interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := codec.NewEncoder(&buf, msgpackHandle).Encode(msg)
	return buf.Bytes(), err
}

func stringHash(s string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(s)))
}

func checkIDHash(checkID types.CheckID) string {
	return stringHash(string(checkID))
}

func setFilePermissions(path string, user, group, mode string) error {
	var err error
	uid, gid := os.Getuid(), os.Getgid()

	if user != "" {
		if uid, err = strconv.Atoi(user); err == nil {
			goto GROUP
		}

		u, err := osuser.Lookup(user)
		if err != nil {
			return fmt.Errorf("failed to look up user %s: %v", user, err)
		}
		uid, _ = strconv.Atoi(u.Uid)
	}

GROUP:
	if group != "" {
		if gid, err = strconv.Atoi(group); err != nil {
			return fmt.Errorf("invalid group specified: %v", group)
		}
	}
	if err := os.Chown(path, uid, gid); err != nil {
		return fmt.Errorf("failed setting ownership to %d:%d on %q: %s",
			uid, gid, path, err)
	}

	if mode != "" {
		mode, err := strconv.ParseUint(mode, 8, 32)
		if err != nil {
			return fmt.Errorf("invalid mode specified: %v", mode)
		}
		if err := os.Chmod(path, os.FileMode(mode)); err != nil {
			return fmt.Errorf("failed setting permissions to %d on %q: %s",
				mode, path, err)
		}
	}

	return nil
}

func ForwardSignals(cmd *exec.Cmd, logFn func(error), shutdownCh <-chan struct{}) {
	go func() {
		signalCh := make(chan os.Signal, 10)
		signal.Notify(signalCh, os.Interrupt, os.Kill)
		defer signal.Stop(signalCh)

		for {
			select {
			case sig := <-signalCh:
				if err := cmd.Process.Signal(sig); err != nil {
					logFn(fmt.Errorf("failed to send signal %q: %v", sig, err))
				}

			case <-shutdownCh:
				return
			}
		}
	}()
}

type durationFixer map[string]bool

func NewDurationFixer(fields ...string) durationFixer {
	d := make(map[string]bool)
	for _, field := range fields {
		d[field] = true
	}
	return d
}

func (d durationFixer) FixupDurations(raw interface{}) error {
	rawMap, ok := raw.(map[string]interface{})
	if !ok {
		return nil
	}
	for key, val := range rawMap {
		switch val.(type) {
		case map[string]interface{}:
			if err := d.FixupDurations(val); err != nil {
				return err
			}

		case []interface{}:
			for _, v := range val.([]interface{}) {
				if err := d.FixupDurations(v); err != nil {
					return err
				}
			}

		case []map[string]interface{}:
			for _, v := range val.([]map[string]interface{}) {
				if err := d.FixupDurations(v); err != nil {
					return err
				}
			}

		default:
			if d[strings.ToLower(key)] {

				if vStr, ok := val.(string); ok {
					dur, err := time.ParseDuration(vStr)
					if err != nil {
						return err
					}
					rawMap[key] = dur
				}
			}
		}
	}
	return nil
}
