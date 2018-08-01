package serf

import (
	"encoding/base64"
	"log"
	"strings"
)

const (
	InternalQueryPrefix = "_serf_"

	pingQuery = "ping"

	conflictQuery = "conflict"

	installKeyQuery = "install-key"

	useKeyQuery = "use-key"

	removeKeyQuery = "remove-key"

	listKeysQuery = "list-keys"
)

func internalQueryName(name string) string {
	return InternalQueryPrefix + name
}

type serfQueries struct {
	inCh       chan Event
	logger     *log.Logger
	outCh      chan<- Event
	serf       *Serf
	shutdownCh <-chan struct{}
}

type nodeKeyResponse struct {
	Result bool

	Message string

	Keys []string
}

func newSerfQueries(serf *Serf, logger *log.Logger, outCh chan<- Event, shutdownCh <-chan struct{}) (chan<- Event, error) {
	inCh := make(chan Event, 1024)
	q := &serfQueries{
		inCh:       inCh,
		logger:     logger,
		outCh:      outCh,
		serf:       serf,
		shutdownCh: shutdownCh,
	}
	go q.stream()
	return inCh, nil
}

func (s *serfQueries) stream() {
	for {
		select {
		case e := <-s.inCh:

			if q, ok := e.(*Query); ok && strings.HasPrefix(q.Name, InternalQueryPrefix) {
				go s.handleQuery(q)

			} else if s.outCh != nil {
				s.outCh <- e
			}

		case <-s.shutdownCh:
			return
		}
	}
}

func (s *serfQueries) handleQuery(q *Query) {

	queryName := q.Name[len(InternalQueryPrefix):]
	switch queryName {
	case pingQuery:

	case conflictQuery:
		s.handleConflict(q)
	case installKeyQuery:
		s.handleInstallKey(q)
	case useKeyQuery:
		s.handleUseKey(q)
	case removeKeyQuery:
		s.handleRemoveKey(q)
	case listKeysQuery:
		s.handleListKeys(q)
	default:
		s.logger.Printf("[WARN] serf: Unhandled internal query '%s'", queryName)
	}
}

func (s *serfQueries) handleConflict(q *Query) {

	node := string(q.Payload)

	if node == s.serf.config.NodeName {
		return
	}
	s.logger.Printf("[DEBUG] serf: Got conflict resolution query for '%s'", node)

	var out *Member
	s.serf.memberLock.Lock()
	if member, ok := s.serf.members[node]; ok {
		out = &member.Member
	}
	s.serf.memberLock.Unlock()

	buf, err := encodeMessage(messageConflictResponseType, out)
	if err != nil {
		s.logger.Printf("[ERR] serf: Failed to encode conflict query response: %v", err)
		return
	}

	if err := q.Respond(buf); err != nil {
		s.logger.Printf("[ERR] serf: Failed to respond to conflict query: %v", err)
	}
}

func (s *serfQueries) sendKeyResponse(q *Query, resp *nodeKeyResponse) {
	buf, err := encodeMessage(messageKeyResponseType, resp)
	if err != nil {
		s.logger.Printf("[ERR] serf: Failed to encode key response: %v", err)
		return
	}

	if err := q.Respond(buf); err != nil {
		s.logger.Printf("[ERR] serf: Failed to respond to key query: %v", err)
		return
	}
}

func (s *serfQueries) handleInstallKey(q *Query) {
	response := nodeKeyResponse{Result: false}
	keyring := s.serf.config.MemberlistConfig.Keyring
	req := keyRequest{}

	err := decodeMessage(q.Payload[1:], &req)
	if err != nil {
		s.logger.Printf("[ERR] serf: Failed to decode key request: %v", err)
		goto SEND
	}

	if !s.serf.EncryptionEnabled() {
		response.Message = "No keyring to modify (encryption not enabled)"
		s.logger.Printf("[ERR] serf: No keyring to modify (encryption not enabled)")
		goto SEND
	}

	s.logger.Printf("[INFO] serf: Received install-key query")
	if err := keyring.AddKey(req.Key); err != nil {
		response.Message = err.Error()
		s.logger.Printf("[ERR] serf: Failed to install key: %s", err)
		goto SEND
	}

	if s.serf.config.KeyringFile != "" {
		if err := s.serf.writeKeyringFile(); err != nil {
			response.Message = err.Error()
			s.logger.Printf("[ERR] serf: Failed to write keyring file: %s", err)
			goto SEND
		}
	}

	response.Result = true

SEND:
	s.sendKeyResponse(q, &response)
}

func (s *serfQueries) handleUseKey(q *Query) {
	response := nodeKeyResponse{Result: false}
	keyring := s.serf.config.MemberlistConfig.Keyring
	req := keyRequest{}

	err := decodeMessage(q.Payload[1:], &req)
	if err != nil {
		s.logger.Printf("[ERR] serf: Failed to decode key request: %v", err)
		goto SEND
	}

	if !s.serf.EncryptionEnabled() {
		response.Message = "No keyring to modify (encryption not enabled)"
		s.logger.Printf("[ERR] serf: No keyring to modify (encryption not enabled)")
		goto SEND
	}

	s.logger.Printf("[INFO] serf: Received use-key query")
	if err := keyring.UseKey(req.Key); err != nil {
		response.Message = err.Error()
		s.logger.Printf("[ERR] serf: Failed to change primary key: %s", err)
		goto SEND
	}

	if err := s.serf.writeKeyringFile(); err != nil {
		response.Message = err.Error()
		s.logger.Printf("[ERR] serf: Failed to write keyring file: %s", err)
		goto SEND
	}

	response.Result = true

SEND:
	s.sendKeyResponse(q, &response)
}

func (s *serfQueries) handleRemoveKey(q *Query) {
	response := nodeKeyResponse{Result: false}
	keyring := s.serf.config.MemberlistConfig.Keyring
	req := keyRequest{}

	err := decodeMessage(q.Payload[1:], &req)
	if err != nil {
		s.logger.Printf("[ERR] serf: Failed to decode key request: %v", err)
		goto SEND
	}

	if !s.serf.EncryptionEnabled() {
		response.Message = "No keyring to modify (encryption not enabled)"
		s.logger.Printf("[ERR] serf: No keyring to modify (encryption not enabled)")
		goto SEND
	}

	s.logger.Printf("[INFO] serf: Received remove-key query")
	if err := keyring.RemoveKey(req.Key); err != nil {
		response.Message = err.Error()
		s.logger.Printf("[ERR] serf: Failed to remove key: %s", err)
		goto SEND
	}

	if err := s.serf.writeKeyringFile(); err != nil {
		response.Message = err.Error()
		s.logger.Printf("[ERR] serf: Failed to write keyring file: %s", err)
		goto SEND
	}

	response.Result = true

SEND:
	s.sendKeyResponse(q, &response)
}

func (s *serfQueries) handleListKeys(q *Query) {
	response := nodeKeyResponse{Result: false}
	keyring := s.serf.config.MemberlistConfig.Keyring

	if !s.serf.EncryptionEnabled() {
		response.Message = "Keyring is empty (encryption not enabled)"
		s.logger.Printf("[ERR] serf: Keyring is empty (encryption not enabled)")
		goto SEND
	}

	s.logger.Printf("[INFO] serf: Received list-keys query")
	for _, keyBytes := range keyring.GetKeys() {

		key := base64.StdEncoding.EncodeToString(keyBytes)
		response.Keys = append(response.Keys, key)
	}
	response.Result = true

SEND:
	s.sendKeyResponse(q, &response)
}
