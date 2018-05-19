package token

import (
	"sync"
)

type Store struct {
	l sync.RWMutex

	userToken string

	agentToken string

	agentMasterToken string

	aclReplicationToken string
}

func (t *Store) UpdateUserToken(token string) {
	t.l.Lock()
	t.userToken = token
	t.l.Unlock()
}

func (t *Store) UpdateAgentToken(token string) {
	t.l.Lock()
	t.agentToken = token
	t.l.Unlock()
}

func (t *Store) UpdateAgentMasterToken(token string) {
	t.l.Lock()
	t.agentMasterToken = token
	t.l.Unlock()
}

func (t *Store) UpdateACLReplicationToken(token string) {
	t.l.Lock()
	t.aclReplicationToken = token
	t.l.Unlock()
}

func (t *Store) UserToken() string {
	t.l.RLock()
	defer t.l.RUnlock()

	return t.userToken
}

func (t *Store) AgentToken() string {
	t.l.RLock()
	defer t.l.RUnlock()

	if t.agentToken != "" {
		return t.agentToken
	}
	return t.userToken
}

func (t *Store) ACLReplicationToken() string {
	t.l.RLock()
	defer t.l.RUnlock()

	return t.aclReplicationToken
}

func (t *Store) IsAgentMasterToken(token string) bool {
	t.l.RLock()
	defer t.l.RUnlock()

	return (token != "") && (token == t.agentMasterToken)
}
