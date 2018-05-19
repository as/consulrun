package fsm

import (
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/go-msgpack/codec"
	"github.com/hashicorp/raft"
)

var msgpackHandle = &codec.MsgpackHandle{}

type command func(buf []byte, index uint64) interface{}

type unboundCommand func(c *FSM, buf []byte, index uint64) interface{}

var commands map[structs.MessageType]unboundCommand

func registerCommand(msg structs.MessageType, fn unboundCommand) {
	if commands == nil {
		commands = make(map[structs.MessageType]unboundCommand)
	}
	if commands[msg] != nil {
		panic(fmt.Errorf("Message %d is already registered", msg))
	}
	commands[msg] = fn
}

type FSM struct {
	logOutput io.Writer
	logger    *log.Logger
	path      string

	apply map[structs.MessageType]command

	stateLock sync.RWMutex
	state     *state.Store

	gc *state.TombstoneGC
}

func New(gc *state.TombstoneGC, logOutput io.Writer) (*FSM, error) {
	stateNew, err := state.NewStateStore(gc)
	if err != nil {
		return nil, err
	}

	fsm := &FSM{
		logOutput: logOutput,
		logger:    log.New(logOutput, "", log.LstdFlags),
		apply:     make(map[structs.MessageType]command),
		state:     stateNew,
		gc:        gc,
	}

	for msg, fn := range commands {
		thisFn := fn
		fsm.apply[msg] = func(buf []byte, index uint64) interface{} {
			return thisFn(fsm, buf, index)
		}
	}

	return fsm, nil
}

func (c *FSM) State() *state.Store {
	c.stateLock.RLock()
	defer c.stateLock.RUnlock()
	return c.state
}

func (c *FSM) Apply(log *raft.Log) interface{} {
	buf := log.Data
	msgType := structs.MessageType(buf[0])

	ignoreUnknown := false
	if msgType&structs.IgnoreUnknownTypeFlag == structs.IgnoreUnknownTypeFlag {
		msgType &= ^structs.IgnoreUnknownTypeFlag
		ignoreUnknown = true
	}

	if fn := c.apply[msgType]; fn != nil {
		return fn(buf[1:], log.Index)
	}

	if ignoreUnknown {
		c.logger.Printf("[WARN] consul.fsm: ignoring unknown message type (%d), upgrade to newer version", msgType)
		return nil
	}
	panic(fmt.Errorf("failed to apply request: %#v", buf))
}

func (c *FSM) Snapshot() (raft.FSMSnapshot, error) {
	defer func(start time.Time) {
		c.logger.Printf("[INFO] consul.fsm: snapshot created in %v", time.Since(start))
	}(time.Now())

	return &snapshot{c.state.Snapshot()}, nil
}

func (c *FSM) Restore(old io.ReadCloser) error {
	defer old.Close()

	stateNew, err := state.NewStateStore(c.gc)
	if err != nil {
		return err
	}

	restore := stateNew.Restore()
	defer restore.Abort()

	dec := codec.NewDecoder(old, msgpackHandle)

	var header snapshotHeader
	if err := dec.Decode(&header); err != nil {
		return err
	}

	msgType := make([]byte, 1)
	for {

		_, err := old.Read(msgType)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		msg := structs.MessageType(msgType[0])
		if fn := restorers[msg]; fn != nil {
			if err := fn(&header, restore, dec); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("Unrecognized msg type %d", msg)
		}
	}
	restore.Commit()

	c.stateLock.Lock()
	stateOld := c.state
	c.state = stateNew
	c.stateLock.Unlock()

	stateOld.Abandon()
	return nil
}
