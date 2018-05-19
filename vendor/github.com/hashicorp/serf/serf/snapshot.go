package serf

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/armon/go-metrics"
)

/*
Serf supports using a "snapshot" file that contains various
transactional data that is used to help Serf recover quickly
and gracefully from a failure. We append member events, as well
as the latest clock values to the file during normal operation,
and periodically checkpoint and roll over the file. During a restore,
we can replay the various member events to recall a list of known
nodes to re-join, as well as restore our clock values to avoid replaying
old events.
*/

const flushInterval = 500 * time.Millisecond
const clockUpdateInterval = 500 * time.Millisecond
const tmpExt = ".compact"
const snapshotErrorRecoveryInterval = 30 * time.Second

type Snapshotter struct {
	aliveNodes              map[string]string
	clock                   *LamportClock
	fh                      *os.File
	buffered                *bufio.Writer
	inCh                    <-chan Event
	lastFlush               time.Time
	lastClock               LamportTime
	lastEventClock          LamportTime
	lastQueryClock          LamportTime
	leaveCh                 chan struct{}
	leaving                 bool
	logger                  *log.Logger
	maxSize                 int64
	path                    string
	offset                  int64
	outCh                   chan<- Event
	rejoinAfterLeave        bool
	shutdownCh              <-chan struct{}
	waitCh                  chan struct{}
	lastAttemptedCompaction time.Time
}

type PreviousNode struct {
	Name string
	Addr string
}

func (p PreviousNode) String() string {
	return fmt.Sprintf("%s: %s", p.Name, p.Addr)
}

func NewSnapshotter(path string,
	maxSize int,
	rejoinAfterLeave bool,
	logger *log.Logger,
	clock *LamportClock,
	outCh chan<- Event,
	shutdownCh <-chan struct{}) (chan<- Event, *Snapshotter, error) {
	inCh := make(chan Event, 1024)

	fh, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open snapshot: %v", err)
	}

	info, err := fh.Stat()
	if err != nil {
		fh.Close()
		return nil, nil, fmt.Errorf("failed to stat snapshot: %v", err)
	}
	offset := info.Size()

	snap := &Snapshotter{
		aliveNodes:       make(map[string]string),
		clock:            clock,
		fh:               fh,
		buffered:         bufio.NewWriter(fh),
		inCh:             inCh,
		lastClock:        0,
		lastEventClock:   0,
		lastQueryClock:   0,
		leaveCh:          make(chan struct{}),
		logger:           logger,
		maxSize:          int64(maxSize),
		path:             path,
		offset:           offset,
		outCh:            outCh,
		rejoinAfterLeave: rejoinAfterLeave,
		shutdownCh:       shutdownCh,
		waitCh:           make(chan struct{}),
	}

	if err := snap.replay(); err != nil {
		fh.Close()
		return nil, nil, err
	}

	go snap.stream()
	return inCh, snap, nil
}

func (s *Snapshotter) LastClock() LamportTime {
	return s.lastClock
}

func (s *Snapshotter) LastEventClock() LamportTime {
	return s.lastEventClock
}

func (s *Snapshotter) LastQueryClock() LamportTime {
	return s.lastQueryClock
}

func (s *Snapshotter) AliveNodes() []*PreviousNode {

	previous := make([]*PreviousNode, 0, len(s.aliveNodes))
	for name, addr := range s.aliveNodes {
		previous = append(previous, &PreviousNode{name, addr})
	}

	for i := range previous {
		j := rand.Intn(i + 1)
		previous[i], previous[j] = previous[j], previous[i]
	}
	return previous
}

func (s *Snapshotter) Wait() {
	<-s.waitCh
}

func (s *Snapshotter) Leave() {
	select {
	case s.leaveCh <- struct{}{}:
	case <-s.shutdownCh:
	}
}

func (s *Snapshotter) stream() {
	clockTicker := time.NewTicker(clockUpdateInterval)
	defer clockTicker.Stop()

	for {
		select {
		case <-s.leaveCh:
			s.leaving = true

			if !s.rejoinAfterLeave {
				s.aliveNodes = make(map[string]string)
			}
			s.tryAppend("leave\n")
			if err := s.buffered.Flush(); err != nil {
				s.logger.Printf("[ERR] serf: failed to flush leave to snapshot: %v", err)
			}
			if err := s.fh.Sync(); err != nil {
				s.logger.Printf("[ERR] serf: failed to sync leave to snapshot: %v", err)
			}

		case e := <-s.inCh:

			if s.outCh != nil {
				s.outCh <- e
			}

			if s.leaving {
				continue
			}
			switch typed := e.(type) {
			case MemberEvent:
				s.processMemberEvent(typed)
			case UserEvent:
				s.processUserEvent(typed)
			case *Query:
				s.processQuery(typed)
			default:
				s.logger.Printf("[ERR] serf: Unknown event to snapshot: %#v", e)
			}

		case <-clockTicker.C:
			s.updateClock()

		case <-s.shutdownCh:
			if err := s.buffered.Flush(); err != nil {
				s.logger.Printf("[ERR] serf: failed to flush snapshot: %v", err)
			}
			if err := s.fh.Sync(); err != nil {
				s.logger.Printf("[ERR] serf: failed to sync snapshot: %v", err)
			}
			s.fh.Close()
			close(s.waitCh)
			return
		}
	}
}

func (s *Snapshotter) processMemberEvent(e MemberEvent) {
	switch e.Type {
	case EventMemberJoin:
		for _, mem := range e.Members {
			addr := net.TCPAddr{IP: mem.Addr, Port: int(mem.Port)}
			s.aliveNodes[mem.Name] = addr.String()
			s.tryAppend(fmt.Sprintf("alive: %s %s\n", mem.Name, addr.String()))
		}

	case EventMemberLeave:
		fallthrough
	case EventMemberFailed:
		for _, mem := range e.Members {
			delete(s.aliveNodes, mem.Name)
			s.tryAppend(fmt.Sprintf("not-alive: %s\n", mem.Name))
		}
	}
	s.updateClock()
}

func (s *Snapshotter) updateClock() {
	lastSeen := s.clock.Time() - 1
	if lastSeen > s.lastClock {
		s.lastClock = lastSeen
		s.tryAppend(fmt.Sprintf("clock: %d\n", s.lastClock))
	}
}

func (s *Snapshotter) processUserEvent(e UserEvent) {

	if e.LTime <= s.lastEventClock {
		return
	}
	s.lastEventClock = e.LTime
	s.tryAppend(fmt.Sprintf("event-clock: %d\n", e.LTime))
}

func (s *Snapshotter) processQuery(q *Query) {

	if q.LTime <= s.lastQueryClock {
		return
	}
	s.lastQueryClock = q.LTime
	s.tryAppend(fmt.Sprintf("query-clock: %d\n", q.LTime))
}

func (s *Snapshotter) tryAppend(l string) {
	if err := s.appendLine(l); err != nil {
		s.logger.Printf("[ERR] serf: Failed to update snapshot: %v", err)
		now := time.Now()
		if now.Sub(s.lastAttemptedCompaction) > snapshotErrorRecoveryInterval {
			s.lastAttemptedCompaction = now
			s.logger.Printf("[INFO] serf: Attempting compaction to recover from error...")
			err = s.compact()
			if err != nil {
				s.logger.Printf("[ERR] serf: Compaction failed, will reattempt after %v: %v", snapshotErrorRecoveryInterval, err)
			} else {
				s.logger.Printf("[INFO] serf: Finished compaction, successfully recovered from error state")
			}
		}
	}
}

func (s *Snapshotter) appendLine(l string) error {
	defer metrics.MeasureSince([]string{"serf", "snapshot", "appendLine"}, time.Now())

	n, err := s.buffered.WriteString(l)
	if err != nil {
		return err
	}

	now := time.Now()
	if now.Sub(s.lastFlush) > flushInterval {
		s.lastFlush = now
		if err := s.buffered.Flush(); err != nil {
			return err
		}
	}

	s.offset += int64(n)
	if s.offset > s.maxSize {
		return s.compact()
	}
	return nil
}

func (s *Snapshotter) compact() error {
	defer metrics.MeasureSince([]string{"serf", "snapshot", "compact"}, time.Now())

	newPath := s.path + tmpExt
	fh, err := os.OpenFile(newPath, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		return fmt.Errorf("failed to open new snapshot: %v", err)
	}

	buf := bufio.NewWriter(fh)

	var offset int64
	for name, addr := range s.aliveNodes {
		line := fmt.Sprintf("alive: %s %s\n", name, addr)
		n, err := buf.WriteString(line)
		if err != nil {
			fh.Close()
			return err
		}
		offset += int64(n)
	}

	line := fmt.Sprintf("clock: %d\n", s.lastClock)
	n, err := buf.WriteString(line)
	if err != nil {
		fh.Close()
		return err
	}
	offset += int64(n)

	line = fmt.Sprintf("event-clock: %d\n", s.lastEventClock)
	n, err = buf.WriteString(line)
	if err != nil {
		fh.Close()
		return err
	}
	offset += int64(n)

	line = fmt.Sprintf("query-clock: %d\n", s.lastQueryClock)
	n, err = buf.WriteString(line)
	if err != nil {
		fh.Close()
		return err
	}
	offset += int64(n)

	err = buf.Flush()

	if err != nil {
		return fmt.Errorf("failed to flush new snapshot: %v", err)
	}

	err = fh.Sync()

	if err != nil {
		fh.Close()
		return fmt.Errorf("failed to fsync new snapshot: %v", err)
	}

	fh.Close()

	s.buffered.Flush()
	s.buffered = nil

	s.fh.Close()
	s.fh = nil

	if err := os.Remove(s.path); err != nil {
		return fmt.Errorf("failed to remove old snapshot: %v", err)
	}

	if err := os.Rename(newPath, s.path); err != nil {
		return fmt.Errorf("failed to install new snapshot: %v", err)
	}

	fh, err = os.OpenFile(s.path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return fmt.Errorf("failed to open snapshot: %v", err)
	}
	buf = bufio.NewWriter(fh)

	s.fh = fh
	s.buffered = buf
	s.offset = offset
	s.lastFlush = time.Now()
	return nil
}

func (s *Snapshotter) replay() error {

	if _, err := s.fh.Seek(0, os.SEEK_SET); err != nil {
		return err
	}

	reader := bufio.NewReader(s.fh)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		line = line[:len(line)-1]

		if strings.HasPrefix(line, "alive: ") {
			info := strings.TrimPrefix(line, "alive: ")
			addrIdx := strings.LastIndex(info, " ")
			if addrIdx == -1 {
				s.logger.Printf("[WARN] serf: Failed to parse address: %v", line)
				continue
			}
			addr := info[addrIdx+1:]
			name := info[:addrIdx]
			s.aliveNodes[name] = addr

		} else if strings.HasPrefix(line, "not-alive: ") {
			name := strings.TrimPrefix(line, "not-alive: ")
			delete(s.aliveNodes, name)

		} else if strings.HasPrefix(line, "clock: ") {
			timeStr := strings.TrimPrefix(line, "clock: ")
			timeInt, err := strconv.ParseUint(timeStr, 10, 64)
			if err != nil {
				s.logger.Printf("[WARN] serf: Failed to convert clock time: %v", err)
				continue
			}
			s.lastClock = LamportTime(timeInt)

		} else if strings.HasPrefix(line, "event-clock: ") {
			timeStr := strings.TrimPrefix(line, "event-clock: ")
			timeInt, err := strconv.ParseUint(timeStr, 10, 64)
			if err != nil {
				s.logger.Printf("[WARN] serf: Failed to convert event clock time: %v", err)
				continue
			}
			s.lastEventClock = LamportTime(timeInt)

		} else if strings.HasPrefix(line, "query-clock: ") {
			timeStr := strings.TrimPrefix(line, "query-clock: ")
			timeInt, err := strconv.ParseUint(timeStr, 10, 64)
			if err != nil {
				s.logger.Printf("[WARN] serf: Failed to convert query clock time: %v", err)
				continue
			}
			s.lastQueryClock = LamportTime(timeInt)

		} else if strings.HasPrefix(line, "coordinate: ") {
			continue // Ignores any coordinate persistence from old snapshots, serf should re-converge
		} else if line == "leave" {

			if s.rejoinAfterLeave {
				s.logger.Printf("[INFO] serf: Ignoring previous leave in snapshot")
				continue
			}
			s.aliveNodes = make(map[string]string)
			s.lastClock = 0
			s.lastEventClock = 0
			s.lastQueryClock = 0

		} else if strings.HasPrefix(line, "#") {

		} else {
			s.logger.Printf("[WARN] serf: Unrecognized snapshot line: %v", line)
		}
	}

	if _, err := s.fh.Seek(0, os.SEEK_END); err != nil {
		return err
	}
	return nil
}
