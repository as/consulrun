package raft

import (
	"fmt"
	"io"
	"sync"
	"time"
)

type Future interface {
	Error() error
}

type IndexFuture interface {
	Future

	Index() uint64
}

type ApplyFuture interface {
	IndexFuture

	Response() interface{}
}

type ConfigurationFuture interface {
	IndexFuture

	Configuration() Configuration
}

type SnapshotFuture interface {
	Future

	Open() (*SnapshotMeta, io.ReadCloser, error)
}

type errorFuture struct {
	err error
}

func (e errorFuture) Error() error {
	return e.err
}

func (e errorFuture) Response() interface{} {
	return nil
}

func (e errorFuture) Index() uint64 {
	return 0
}

type deferError struct {
	err       error
	errCh     chan error
	responded bool
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) Error() error {
	if d.err != nil {

		return d.err
	}
	if d.errCh == nil {
		panic("waiting for response on nil channel")
	}
	d.err = <-d.errCh
	return d.err
}

func (d *deferError) respond(err error) {
	if d.errCh == nil {
		return
	}
	if d.responded {
		return
	}
	d.errCh <- err
	close(d.errCh)
	d.responded = true
}

type configurationChangeFuture struct {
	logFuture
	req configurationChangeRequest
}

type bootstrapFuture struct {
	deferError

	configuration Configuration
}

type logFuture struct {
	deferError
	log      Log
	response interface{}
	dispatch time.Time
}

func (l *logFuture) Response() interface{} {
	return l.response
}

func (l *logFuture) Index() uint64 {
	return l.log.Index
}

type shutdownFuture struct {
	raft *Raft
}

func (s *shutdownFuture) Error() error {
	if s.raft == nil {
		return nil
	}
	s.raft.waitShutdown()
	if closeable, ok := s.raft.trans.(WithClose); ok {
		closeable.Close()
	}
	return nil
}

type userSnapshotFuture struct {
	deferError

	opener func() (*SnapshotMeta, io.ReadCloser, error)
}

func (u *userSnapshotFuture) Open() (*SnapshotMeta, io.ReadCloser, error) {
	if u.opener == nil {
		return nil, nil, fmt.Errorf("no snapshot available")
	} else {

		defer func() {
			u.opener = nil
		}()
		return u.opener()
	}
}

type userRestoreFuture struct {
	deferError

	meta *SnapshotMeta

	reader io.Reader
}

type reqSnapshotFuture struct {
	deferError

	index    uint64
	term     uint64
	snapshot FSMSnapshot
}

type restoreFuture struct {
	deferError
	ID string
}

type verifyFuture struct {
	deferError
	notifyCh   chan *verifyFuture
	quorumSize int
	votes      int
	voteLock   sync.Mutex
}

type configurationsFuture struct {
	deferError
	configurations configurations
}

func (c *configurationsFuture) Configuration() Configuration {
	return c.configurations.latest
}

func (c *configurationsFuture) Index() uint64 {
	return c.configurations.latestIndex
}

func (v *verifyFuture) vote(leader bool) {
	v.voteLock.Lock()
	defer v.voteLock.Unlock()

	if v.notifyCh == nil {
		return
	}

	if leader {
		v.votes++
		if v.votes >= v.quorumSize {
			v.notifyCh <- v
			v.notifyCh = nil
		}
	} else {
		v.notifyCh <- v
		v.notifyCh = nil
	}
}

type appendFuture struct {
	deferError
	start time.Time
	args  *AppendEntriesRequest
	resp  *AppendEntriesResponse
}

func (a *appendFuture) Start() time.Time {
	return a.start
}

func (a *appendFuture) Request() *AppendEntriesRequest {
	return a.args
}

func (a *appendFuture) Response() *AppendEntriesResponse {
	return a.resp
}
