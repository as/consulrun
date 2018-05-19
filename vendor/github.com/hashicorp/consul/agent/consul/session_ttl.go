package consul

import (
	"fmt"
	"time"

	"github.com/armon/go-metrics"
	"github.com/hashicorp/consul/agent/structs"
)

const (
	maxInvalidateAttempts = 6

	invalidateRetryBase = 10 * time.Second
)

func (s *Server) initializeSessionTimers() error {

	state := s.fsm.State()
	_, sessions, err := state.SessionList(nil)
	if err != nil {
		return err
	}
	for _, session := range sessions {
		if err := s.resetSessionTimer(session.ID, session); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) resetSessionTimer(id string, session *structs.Session) error {

	if session == nil {
		state := s.fsm.State()
		_, s, err := state.SessionGet(nil, id)
		if err != nil {
			return err
		}
		if s == nil {
			return fmt.Errorf("Session '%s' not found", id)
		}
		session = s
	}

	switch session.TTL {
	case "", "0", "0s", "0m", "0h":
		return nil
	}

	ttl, err := time.ParseDuration(session.TTL)
	if err != nil {
		return fmt.Errorf("Invalid Session TTL '%s': %v", session.TTL, err)
	}
	if ttl == 0 {
		return nil
	}

	s.createSessionTimer(session.ID, ttl)
	return nil
}

func (s *Server) createSessionTimer(id string, ttl time.Duration) {

	ttl = ttl * structs.SessionTTLMultiplier
	s.sessionTimers.ResetOrCreate(id, ttl, func() { s.invalidateSession(id) })
}

func (s *Server) invalidateSession(id string) {
	defer metrics.MeasureSince([]string{"consul", "session_ttl", "invalidate"}, time.Now())
	defer metrics.MeasureSince([]string{"session_ttl", "invalidate"}, time.Now())

	s.sessionTimers.Del(id)

	args := structs.SessionRequest{
		Datacenter: s.config.Datacenter,
		Op:         structs.SessionDestroy,
		Session: structs.Session{
			ID: id,
		},
	}

	for attempt := uint(0); attempt < maxInvalidateAttempts; attempt++ {
		_, err := s.raftApply(structs.SessionRequestType, args)
		if err == nil {
			s.logger.Printf("[DEBUG] consul.state: Session %s TTL expired", id)
			return
		}

		s.logger.Printf("[ERR] consul.session: Invalidation failed: %v", err)
		time.Sleep((1 << attempt) * invalidateRetryBase)
	}
	s.logger.Printf("[ERR] consul.session: maximum revoke attempts reached for session: %s", id)
}

func (s *Server) clearSessionTimer(id string) error {
	s.sessionTimers.Stop(id)
	return nil
}

func (s *Server) clearAllSessionTimers() error {
	s.sessionTimers.StopAll()
	return nil
}

func (s *Server) sessionStats() {
	for {
		select {
		case <-time.After(5 * time.Second):
			metrics.SetGauge([]string{"consul", "session_ttl", "active"}, float32(s.sessionTimers.Len()))
			metrics.SetGauge([]string{"session_ttl", "active"}, float32(s.sessionTimers.Len()))

		case <-s.shutdownCh:
			return
		}
	}
}
