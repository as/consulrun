package yamux

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Session struct {
	remoteGoAway int32

	localGoAway int32

	nextStreamID uint32

	config *Config

	logger *log.Logger

	conn io.ReadWriteCloser

	bufRead *bufio.Reader

	pings    map[uint32]chan struct{}
	pingID   uint32
	pingLock sync.Mutex

	streams    map[uint32]*Stream
	inflight   map[uint32]struct{}
	streamLock sync.Mutex

	synCh chan struct{}

	acceptCh chan *Stream

	sendCh chan sendReady

	recvDoneCh chan struct{}

	shutdown     bool
	shutdownErr  error
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

type sendReady struct {
	Hdr  []byte
	Body io.Reader
	Err  chan error
}

func newSession(config *Config, conn io.ReadWriteCloser, client bool) *Session {
	s := &Session{
		config:     config,
		logger:     log.New(config.LogOutput, "", log.LstdFlags),
		conn:       conn,
		bufRead:    bufio.NewReader(conn),
		pings:      make(map[uint32]chan struct{}),
		streams:    make(map[uint32]*Stream),
		inflight:   make(map[uint32]struct{}),
		synCh:      make(chan struct{}, config.AcceptBacklog),
		acceptCh:   make(chan *Stream, config.AcceptBacklog),
		sendCh:     make(chan sendReady, 64),
		recvDoneCh: make(chan struct{}),
		shutdownCh: make(chan struct{}),
	}
	if client {
		s.nextStreamID = 1
	} else {
		s.nextStreamID = 2
	}
	go s.recv()
	go s.send()
	if config.EnableKeepAlive {
		go s.keepalive()
	}
	return s
}

func (s *Session) IsClosed() bool {
	select {
	case <-s.shutdownCh:
		return true
	default:
		return false
	}
}

func (s *Session) CloseChan() <-chan struct{} {
	return s.shutdownCh
}

func (s *Session) NumStreams() int {
	s.streamLock.Lock()
	num := len(s.streams)
	s.streamLock.Unlock()
	return num
}

func (s *Session) Open() (net.Conn, error) {
	conn, err := s.OpenStream()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (s *Session) OpenStream() (*Stream, error) {
	if s.IsClosed() {
		return nil, ErrSessionShutdown
	}
	if atomic.LoadInt32(&s.remoteGoAway) == 1 {
		return nil, ErrRemoteGoAway
	}

	select {
	case s.synCh <- struct{}{}:
	case <-s.shutdownCh:
		return nil, ErrSessionShutdown
	}

GET_ID:

	id := atomic.LoadUint32(&s.nextStreamID)
	if id >= math.MaxUint32-1 {
		return nil, ErrStreamsExhausted
	}
	if !atomic.CompareAndSwapUint32(&s.nextStreamID, id, id+2) {
		goto GET_ID
	}

	stream := newStream(s, id, streamInit)
	s.streamLock.Lock()
	s.streams[id] = stream
	s.inflight[id] = struct{}{}
	s.streamLock.Unlock()

	if err := stream.sendWindowUpdate(); err != nil {
		select {
		case <-s.synCh:
		default:
			s.logger.Printf("[ERR] yamux: aborted stream open without inflight syn semaphore")
		}
		return nil, err
	}
	return stream, nil
}

func (s *Session) Accept() (net.Conn, error) {
	conn, err := s.AcceptStream()
	if err != nil {
		return nil, err
	}
	return conn, err
}

func (s *Session) AcceptStream() (*Stream, error) {
	select {
	case stream := <-s.acceptCh:
		if err := stream.sendWindowUpdate(); err != nil {
			return nil, err
		}
		return stream, nil
	case <-s.shutdownCh:
		return nil, s.shutdownErr
	}
}

func (s *Session) Close() error {
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	if s.shutdown {
		return nil
	}
	s.shutdown = true
	if s.shutdownErr == nil {
		s.shutdownErr = ErrSessionShutdown
	}
	close(s.shutdownCh)
	s.conn.Close()
	<-s.recvDoneCh

	s.streamLock.Lock()
	defer s.streamLock.Unlock()
	for _, stream := range s.streams {
		stream.forceClose()
	}
	return nil
}

func (s *Session) exitErr(err error) {
	s.shutdownLock.Lock()
	if s.shutdownErr == nil {
		s.shutdownErr = err
	}
	s.shutdownLock.Unlock()
	s.Close()
}

func (s *Session) GoAway() error {
	return s.waitForSend(s.goAway(goAwayNormal), nil)
}

func (s *Session) goAway(reason uint32) header {
	atomic.SwapInt32(&s.localGoAway, 1)
	hdr := header(make([]byte, headerSize))
	hdr.encode(typeGoAway, 0, 0, reason)
	return hdr
}

func (s *Session) Ping() (time.Duration, error) {

	ch := make(chan struct{})

	s.pingLock.Lock()
	id := s.pingID
	s.pingID++
	s.pings[id] = ch
	s.pingLock.Unlock()

	hdr := header(make([]byte, headerSize))
	hdr.encode(typePing, flagSYN, 0, id)
	if err := s.waitForSend(hdr, nil); err != nil {
		return 0, err
	}

	start := time.Now()
	select {
	case <-ch:
	case <-time.After(s.config.ConnectionWriteTimeout):
		s.pingLock.Lock()
		delete(s.pings, id) // Ignore it if a response comes later.
		s.pingLock.Unlock()
		return 0, ErrTimeout
	case <-s.shutdownCh:
		return 0, ErrSessionShutdown
	}

	return time.Now().Sub(start), nil
}

func (s *Session) keepalive() {
	for {
		select {
		case <-time.After(s.config.KeepAliveInterval):
			_, err := s.Ping()
			if err != nil {
				s.logger.Printf("[ERR] yamux: keepalive failed: %v", err)
				s.exitErr(ErrKeepAliveTimeout)
				return
			}
		case <-s.shutdownCh:
			return
		}
	}
}

func (s *Session) waitForSend(hdr header, body io.Reader) error {
	errCh := make(chan error, 1)
	return s.waitForSendErr(hdr, body, errCh)
}

func (s *Session) waitForSendErr(hdr header, body io.Reader, errCh chan error) error {
	t := timerPool.Get()
	timer := t.(*time.Timer)
	timer.Reset(s.config.ConnectionWriteTimeout)
	defer func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
		timerPool.Put(t)
	}()

	ready := sendReady{Hdr: hdr, Body: body, Err: errCh}
	select {
	case s.sendCh <- ready:
	case <-s.shutdownCh:
		return ErrSessionShutdown
	case <-timer.C:
		return ErrConnectionWriteTimeout
	}

	select {
	case err := <-errCh:
		return err
	case <-s.shutdownCh:
		return ErrSessionShutdown
	case <-timer.C:
		return ErrConnectionWriteTimeout
	}
}

func (s *Session) sendNoWait(hdr header) error {
	t := timerPool.Get()
	timer := t.(*time.Timer)
	timer.Reset(s.config.ConnectionWriteTimeout)
	defer func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
		timerPool.Put(t)
	}()

	select {
	case s.sendCh <- sendReady{Hdr: hdr}:
		return nil
	case <-s.shutdownCh:
		return ErrSessionShutdown
	case <-timer.C:
		return ErrConnectionWriteTimeout
	}
}

func (s *Session) send() {
	for {
		select {
		case ready := <-s.sendCh:

			if ready.Hdr != nil {
				sent := 0
				for sent < len(ready.Hdr) {
					n, err := s.conn.Write(ready.Hdr[sent:])
					if err != nil {
						s.logger.Printf("[ERR] yamux: Failed to write header: %v", err)
						asyncSendErr(ready.Err, err)
						s.exitErr(err)
						return
					}
					sent += n
				}
			}

			if ready.Body != nil {
				_, err := io.Copy(s.conn, ready.Body)
				if err != nil {
					s.logger.Printf("[ERR] yamux: Failed to write body: %v", err)
					asyncSendErr(ready.Err, err)
					s.exitErr(err)
					return
				}
			}

			asyncSendErr(ready.Err, nil)
		case <-s.shutdownCh:
			return
		}
	}
}

func (s *Session) recv() {
	if err := s.recvLoop(); err != nil {
		s.exitErr(err)
	}
}

var (
	handlers = []func(*Session, header) error{
		typeData:         (*Session).handleStreamMessage,
		typeWindowUpdate: (*Session).handleStreamMessage,
		typePing:         (*Session).handlePing,
		typeGoAway:       (*Session).handleGoAway,
	}
)

func (s *Session) recvLoop() error {
	defer close(s.recvDoneCh)
	hdr := header(make([]byte, headerSize))
	for {

		if _, err := io.ReadFull(s.bufRead, hdr); err != nil {
			if err != io.EOF && !strings.Contains(err.Error(), "closed") && !strings.Contains(err.Error(), "reset by peer") {
				s.logger.Printf("[ERR] yamux: Failed to read header: %v", err)
			}
			return err
		}

		if hdr.Version() != protoVersion {
			s.logger.Printf("[ERR] yamux: Invalid protocol version: %d", hdr.Version())
			return ErrInvalidVersion
		}

		mt := hdr.MsgType()
		if mt < typeData || mt > typeGoAway {
			return ErrInvalidMsgType
		}

		if err := handlers[mt](s, hdr); err != nil {
			return err
		}
	}
}

// handleStreamMessage handles either a data or window update frame
func (s *Session) handleStreamMessage(hdr header) error {

	id := hdr.StreamID()
	flags := hdr.Flags()
	if flags&flagSYN == flagSYN {
		if err := s.incomingStream(id); err != nil {
			return err
		}
	}

	s.streamLock.Lock()
	stream := s.streams[id]
	s.streamLock.Unlock()

	if stream == nil {

		if hdr.MsgType() == typeData && hdr.Length() > 0 {
			s.logger.Printf("[WARN] yamux: Discarding data for stream: %d", id)
			if _, err := io.CopyN(ioutil.Discard, s.bufRead, int64(hdr.Length())); err != nil {
				s.logger.Printf("[ERR] yamux: Failed to discard data: %v", err)
				return nil
			}
		} else {
			s.logger.Printf("[WARN] yamux: frame for missing stream: %v", hdr)
		}
		return nil
	}

	if hdr.MsgType() == typeWindowUpdate {
		if err := stream.incrSendWindow(hdr, flags); err != nil {
			if sendErr := s.sendNoWait(s.goAway(goAwayProtoErr)); sendErr != nil {
				s.logger.Printf("[WARN] yamux: failed to send go away: %v", sendErr)
			}
			return err
		}
		return nil
	}

	if err := stream.readData(hdr, flags, s.bufRead); err != nil {
		if sendErr := s.sendNoWait(s.goAway(goAwayProtoErr)); sendErr != nil {
			s.logger.Printf("[WARN] yamux: failed to send go away: %v", sendErr)
		}
		return err
	}
	return nil
}

// handlePing is invokde for a typePing frame
func (s *Session) handlePing(hdr header) error {
	flags := hdr.Flags()
	pingID := hdr.Length()

	if flags&flagSYN == flagSYN {
		go func() {
			hdr := header(make([]byte, headerSize))
			hdr.encode(typePing, flagACK, 0, pingID)
			if err := s.sendNoWait(hdr); err != nil {
				s.logger.Printf("[WARN] yamux: failed to send ping reply: %v", err)
			}
		}()
		return nil
	}

	s.pingLock.Lock()
	ch := s.pings[pingID]
	if ch != nil {
		delete(s.pings, pingID)
		close(ch)
	}
	s.pingLock.Unlock()
	return nil
}

// handleGoAway is invokde for a typeGoAway frame
func (s *Session) handleGoAway(hdr header) error {
	code := hdr.Length()
	switch code {
	case goAwayNormal:
		atomic.SwapInt32(&s.remoteGoAway, 1)
	case goAwayProtoErr:
		s.logger.Printf("[ERR] yamux: received protocol error go away")
		return fmt.Errorf("yamux protocol error")
	case goAwayInternalErr:
		s.logger.Printf("[ERR] yamux: received internal error go away")
		return fmt.Errorf("remote yamux internal error")
	default:
		s.logger.Printf("[ERR] yamux: received unexpected go away")
		return fmt.Errorf("unexpected go away received")
	}
	return nil
}

func (s *Session) incomingStream(id uint32) error {

	if atomic.LoadInt32(&s.localGoAway) == 1 {
		hdr := header(make([]byte, headerSize))
		hdr.encode(typeWindowUpdate, flagRST, id, 0)
		return s.sendNoWait(hdr)
	}

	stream := newStream(s, id, streamSYNReceived)

	s.streamLock.Lock()
	defer s.streamLock.Unlock()

	if _, ok := s.streams[id]; ok {
		s.logger.Printf("[ERR] yamux: duplicate stream declared")
		if sendErr := s.sendNoWait(s.goAway(goAwayProtoErr)); sendErr != nil {
			s.logger.Printf("[WARN] yamux: failed to send go away: %v", sendErr)
		}
		return ErrDuplicateStream
	}

	s.streams[id] = stream

	select {
	case s.acceptCh <- stream:
		return nil
	default:

		s.logger.Printf("[WARN] yamux: backlog exceeded, forcing connection reset")
		delete(s.streams, id)
		stream.sendHdr.encode(typeWindowUpdate, flagRST, id, 0)
		return s.sendNoWait(stream.sendHdr)
	}
}

func (s *Session) closeStream(id uint32) {
	s.streamLock.Lock()
	if _, ok := s.inflight[id]; ok {
		select {
		case <-s.synCh:
		default:
			s.logger.Printf("[ERR] yamux: SYN tracking out of sync")
		}
	}
	delete(s.streams, id)
	s.streamLock.Unlock()
}

func (s *Session) establishStream(id uint32) {
	s.streamLock.Lock()
	if _, ok := s.inflight[id]; ok {
		delete(s.inflight, id)
	} else {
		s.logger.Printf("[ERR] yamux: established stream without inflight SYN (no tracking entry)")
	}
	select {
	case <-s.synCh:
	default:
		s.logger.Printf("[ERR] yamux: established stream without inflight SYN (didn't have semaphore)")
	}
	s.streamLock.Unlock()
}
