package state

import (
	"fmt"
	"time"

	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/api"
	"github.com/as/consulrun/hashicorp/go-memdb"
)

func sessionsTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "sessions",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.UUIDFieldIndex{
					Field: "ID",
				},
			},
			"node": {
				Name:         "node",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Node",
					Lowercase: true,
				},
			},
		},
	}
}

func sessionChecksTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "session_checks",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field:     "Node",
							Lowercase: true,
						},
						&memdb.StringFieldIndex{
							Field:     "CheckID",
							Lowercase: true,
						},
						&memdb.UUIDFieldIndex{
							Field: "Session",
						},
					},
				},
			},
			"node_check": {
				Name:         "node_check",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.CompoundIndex{
					Indexes: []memdb.Indexer{
						&memdb.StringFieldIndex{
							Field:     "Node",
							Lowercase: true,
						},
						&memdb.StringFieldIndex{
							Field:     "CheckID",
							Lowercase: true,
						},
					},
				},
			},
			"session": {
				Name:         "session",
				AllowMissing: false,
				Unique:       false,
				Indexer: &memdb.UUIDFieldIndex{
					Field: "Session",
				},
			},
		},
	}
}

func init() {
	registerSchema(sessionsTableSchema)
	registerSchema(sessionChecksTableSchema)
}

func (s *Snapshot) Sessions() (memdb.ResultIterator, error) {
	iter, err := s.tx.Get("sessions", "id")
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (s *Restore) Session(sess *structs.Session) error {

	if err := s.tx.Insert("sessions", sess); err != nil {
		return fmt.Errorf("failed inserting session: %s", err)
	}

	for _, checkID := range sess.Checks {
		mapping := &sessionCheck{
			Node:    sess.Node,
			CheckID: checkID,
			Session: sess.ID,
		}
		if err := s.tx.Insert("session_checks", mapping); err != nil {
			return fmt.Errorf("failed inserting session check mapping: %s", err)
		}
	}

	if err := indexUpdateMaxTxn(s.tx, sess.ModifyIndex, "sessions"); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

func (s *Store) SessionCreate(idx uint64, sess *structs.Session) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.sessionCreateTxn(tx, idx, sess); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) sessionCreateTxn(tx *memdb.Txn, idx uint64, sess *structs.Session) error {

	if sess.ID == "" {
		return ErrMissingSessionID
	}

	switch sess.Behavior {
	case "":

		sess.Behavior = structs.SessionKeysRelease
	case structs.SessionKeysRelease:
	case structs.SessionKeysDelete:
	default:
		return fmt.Errorf("Invalid session behavior: %s", sess.Behavior)
	}

	sess.CreateIndex = idx
	sess.ModifyIndex = idx

	node, err := tx.First("nodes", "id", sess.Node)
	if err != nil {
		return fmt.Errorf("failed node lookup: %s", err)
	}
	if node == nil {
		return ErrMissingNode
	}

	for _, checkID := range sess.Checks {
		check, err := tx.First("checks", "id", sess.Node, string(checkID))
		if err != nil {
			return fmt.Errorf("failed check lookup: %s", err)
		}
		if check == nil {
			return fmt.Errorf("Missing check '%s' registration", checkID)
		}

		status := check.(*structs.HealthCheck).Status
		if status == api.HealthCritical {
			return fmt.Errorf("Check '%s' is in %s state", checkID, status)
		}
	}

	if err := tx.Insert("sessions", sess); err != nil {
		return fmt.Errorf("failed inserting session: %s", err)
	}

	for _, checkID := range sess.Checks {
		mapping := &sessionCheck{
			Node:    sess.Node,
			CheckID: checkID,
			Session: sess.ID,
		}
		if err := tx.Insert("session_checks", mapping); err != nil {
			return fmt.Errorf("failed inserting session check mapping: %s", err)
		}
	}

	if err := tx.Insert("index", &IndexEntry{"sessions", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

func (s *Store) SessionGet(ws memdb.WatchSet, sessionID string) (uint64, *structs.Session, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "sessions")

	watchCh, session, err := tx.FirstWatch("sessions", "id", sessionID)
	if err != nil {
		return 0, nil, fmt.Errorf("failed session lookup: %s", err)
	}
	ws.Add(watchCh)
	if session != nil {
		return idx, session.(*structs.Session), nil
	}
	return idx, nil, nil
}

func (s *Store) SessionList(ws memdb.WatchSet) (uint64, structs.Sessions, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "sessions")

	sessions, err := tx.Get("sessions", "id")
	if err != nil {
		return 0, nil, fmt.Errorf("failed session lookup: %s", err)
	}
	ws.Add(sessions.WatchCh())

	var result structs.Sessions
	for session := sessions.Next(); session != nil; session = sessions.Next() {
		result = append(result, session.(*structs.Session))
	}
	return idx, result, nil
}

func (s *Store) NodeSessions(ws memdb.WatchSet, nodeID string) (uint64, structs.Sessions, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "sessions")

	sessions, err := tx.Get("sessions", "node", nodeID)
	if err != nil {
		return 0, nil, fmt.Errorf("failed session lookup: %s", err)
	}
	ws.Add(sessions.WatchCh())

	var result structs.Sessions
	for session := sessions.Next(); session != nil; session = sessions.Next() {
		result = append(result, session.(*structs.Session))
	}
	return idx, result, nil
}

func (s *Store) SessionDestroy(idx uint64, sessionID string) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.deleteSessionTxn(tx, idx, sessionID); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) deleteSessionTxn(tx *memdb.Txn, idx uint64, sessionID string) error {

	sess, err := tx.First("sessions", "id", sessionID)
	if err != nil {
		return fmt.Errorf("failed session lookup: %s", err)
	}
	if sess == nil {
		return nil
	}

	if err := tx.Delete("sessions", sess); err != nil {
		return fmt.Errorf("failed deleting session: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{"sessions", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	session := sess.(*structs.Session)
	delay := session.LockDelay
	if delay > structs.MaxLockDelay {
		delay = structs.MaxLockDelay
	}

	now := time.Now()

	entries, err := tx.Get("kvs", "session", sessionID)
	if err != nil {
		return fmt.Errorf("failed kvs lookup: %s", err)
	}
	var kvs []interface{}
	for entry := entries.Next(); entry != nil; entry = entries.Next() {
		kvs = append(kvs, entry)
	}

	switch session.Behavior {
	case structs.SessionKeysRelease:
		for _, obj := range kvs {

			e := obj.(*structs.DirEntry).Clone()
			e.Session = ""
			if err := s.kvsSetTxn(tx, idx, e, true); err != nil {
				return fmt.Errorf("failed kvs update: %s", err)
			}

			if delay > 0 {
				s.lockDelay.SetExpiration(e.Key, now, delay)
			}
		}
	case structs.SessionKeysDelete:
		for _, obj := range kvs {
			e := obj.(*structs.DirEntry)
			if err := s.kvsDeleteTxn(tx, idx, e.Key); err != nil {
				return fmt.Errorf("failed kvs delete: %s", err)
			}

			if delay > 0 {
				s.lockDelay.SetExpiration(e.Key, now, delay)
			}
		}
	default:
		return fmt.Errorf("unknown session behavior %#v", session.Behavior)
	}

	mappings, err := tx.Get("session_checks", "session", sessionID)
	if err != nil {
		return fmt.Errorf("failed session checks lookup: %s", err)
	}
	{
		var objs []interface{}
		for mapping := mappings.Next(); mapping != nil; mapping = mappings.Next() {
			objs = append(objs, mapping)
		}

		for _, obj := range objs {
			if err := tx.Delete("session_checks", obj); err != nil {
				return fmt.Errorf("failed deleting session check: %s", err)
			}
		}
	}

	queries, err := tx.Get("prepared-queries", "session", sessionID)
	if err != nil {
		return fmt.Errorf("failed prepared query lookup: %s", err)
	}
	{
		var ids []string
		for wrapped := queries.Next(); wrapped != nil; wrapped = queries.Next() {
			ids = append(ids, toPreparedQuery(wrapped).ID)
		}

		for _, id := range ids {
			if err := s.preparedQueryDeleteTxn(tx, idx, id); err != nil {
				return fmt.Errorf("failed prepared query delete: %s", err)
			}
		}
	}

	return nil
}
