package state

import (
	"fmt"

	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/go-memdb"
)

func aclsTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "acls",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.StringFieldIndex{
					Field:     "ID",
					Lowercase: false,
				},
			},
		},
	}
}

func aclsBootstrapTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "acls-bootstrap",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: true,
				Unique:       true,
				Indexer: &memdb.ConditionalIndex{
					Conditional: func(obj interface{}) (bool, error) { return true, nil },
				},
			},
		},
	}
}

func init() {
	registerSchema(aclsTableSchema)
	registerSchema(aclsBootstrapTableSchema)
}

func (s *Snapshot) ACLs() (memdb.ResultIterator, error) {
	iter, err := s.tx.Get("acls", "id")
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func (s *Restore) ACL(acl *structs.ACL) error {
	if err := s.tx.Insert("acls", acl); err != nil {
		return fmt.Errorf("failed restoring acl: %s", err)
	}

	if err := indexUpdateMaxTxn(s.tx, acl.ModifyIndex, "acls"); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}
	return nil
}

func (s *Snapshot) ACLBootstrap() (*structs.ACLBootstrap, error) {
	existing, err := s.tx.First("acls-bootstrap", "id")
	if err != nil {
		return nil, fmt.Errorf("failed acl bootstrap lookup: %s", err)
	}
	if existing != nil {
		return existing.(*structs.ACLBootstrap), nil
	}
	return nil, nil
}

func (s *Restore) ACLBootstrap(bs *structs.ACLBootstrap) error {
	if err := s.tx.Insert("acls-bootstrap", bs); err != nil {
		return fmt.Errorf("failed updating acl bootstrap: %v", err)
	}
	return nil
}

//
func (s *Store) ACLBootstrapInit(idx uint64) (bool, error) {
	tx := s.db.Txn(true)
	defer tx.Abort()

	existing, err := tx.First("acls-bootstrap", "id")
	if err != nil {
		return false, fmt.Errorf("failed acl bootstrap lookup: %s", err)
	}
	if existing != nil {
		return false, fmt.Errorf("acl bootstrap init already done")
	}

	foundMgmt, err := s.aclHasManagementTokensTxn(tx)
	if err != nil {
		return false, fmt.Errorf("failed checking for management tokens: %v", err)
	}
	allowBootstrap := !foundMgmt

	bs := structs.ACLBootstrap{
		AllowBootstrap: allowBootstrap,
		RaftIndex: structs.RaftIndex{
			CreateIndex: idx,
			ModifyIndex: idx,
		},
	}
	if err := tx.Insert("acls-bootstrap", &bs); err != nil {
		return false, fmt.Errorf("failed creating acl bootstrap: %v", err)
	}

	tx.Commit()
	return allowBootstrap, nil
}

func (s *Store) ACLBootstrap(idx uint64, acl *structs.ACL) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	existing, err := tx.First("acls-bootstrap", "id")
	if err != nil {
		return fmt.Errorf("failed acl bootstrap lookup: %s", err)
	}
	if existing == nil {
		return structs.ACLBootstrapNotInitializedErr
	}

	bs := *existing.(*structs.ACLBootstrap)
	if !bs.AllowBootstrap {
		return structs.ACLBootstrapNotAllowedErr
	}

	foundMgmt, err := s.aclHasManagementTokensTxn(tx)
	if err != nil {
		return fmt.Errorf("failed checking for management tokens: %v", err)
	}
	if foundMgmt {
		return fmt.Errorf("internal error: acl bootstrap enabled but existing management tokens were found")
	}

	if err := s.aclSetTxn(tx, idx, acl); err != nil {
		return fmt.Errorf("failed inserting bootstrap token: %v", err)
	}
	if disabled, err := s.aclDisableBootstrapTxn(tx, idx); err != nil || !disabled {
		return fmt.Errorf("failed to disable acl bootstrap (disabled=%v): %v", disabled, err)
	}

	tx.Commit()
	return nil
}

func (s *Store) aclDisableBootstrapTxn(tx *memdb.Txn, idx uint64) (bool, error) {

	existing, err := tx.First("acls-bootstrap", "id")
	if err != nil {
		return false, fmt.Errorf("failed acl bootstrap lookup: %s", err)
	}
	if existing == nil {

		return false, nil
	}

	bs := *existing.(*structs.ACLBootstrap)
	if !bs.AllowBootstrap {
		return true, nil
	}

	bs.AllowBootstrap = false
	bs.ModifyIndex = idx
	if err := tx.Insert("acls-bootstrap", &bs); err != nil {
		return false, fmt.Errorf("failed updating acl bootstrap: %v", err)
	}
	return true, nil
}

func (s *Store) aclHasManagementTokensTxn(tx *memdb.Txn) (bool, error) {
	iter, err := tx.Get("acls", "id")
	if err != nil {
		return false, fmt.Errorf("failed acl lookup: %s", err)
	}
	for acl := iter.Next(); acl != nil; acl = iter.Next() {
		if acl.(*structs.ACL).Type == structs.ACLTypeManagement {
			return true, nil
		}
	}
	return false, nil
}

func (s *Store) ACLGetBootstrap() (*structs.ACLBootstrap, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	existing, err := tx.First("acls-bootstrap", "id")
	if err != nil {
		return nil, fmt.Errorf("failed acl bootstrap lookup: %s", err)
	}
	if existing != nil {
		return existing.(*structs.ACLBootstrap), nil
	}
	return nil, nil
}

func (s *Store) ACLSet(idx uint64, acl *structs.ACL) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.aclSetTxn(tx, idx, acl); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) aclSetTxn(tx *memdb.Txn, idx uint64, acl *structs.ACL) error {

	if acl.ID == "" {
		return ErrMissingACLID
	}

	existing, err := tx.First("acls", "id", acl.ID)
	if err != nil {
		return fmt.Errorf("failed acl lookup: %s", err)
	}

	if existing != nil {
		acl.CreateIndex = existing.(*structs.ACL).CreateIndex
		acl.ModifyIndex = idx
	} else {
		acl.CreateIndex = idx
		acl.ModifyIndex = idx
	}

	if err := tx.Insert("acls", acl); err != nil {
		return fmt.Errorf("failed inserting acl: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{"acls", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	if acl.Type == structs.ACLTypeManagement {
		if _, err := s.aclDisableBootstrapTxn(tx, idx); err != nil {
			return fmt.Errorf("failed disabling acl bootstrapping: %v", err)
		}
	}

	return nil
}

func (s *Store) ACLGet(ws memdb.WatchSet, aclID string) (uint64, *structs.ACL, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "acls")

	watchCh, acl, err := tx.FirstWatch("acls", "id", aclID)
	if err != nil {
		return 0, nil, fmt.Errorf("failed acl lookup: %s", err)
	}
	ws.Add(watchCh)

	if acl != nil {
		return idx, acl.(*structs.ACL), nil
	}
	return idx, nil, nil
}

func (s *Store) ACLList(ws memdb.WatchSet) (uint64, structs.ACLs, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "acls")

	acls, err := s.aclListTxn(tx, ws)
	if err != nil {
		return 0, nil, fmt.Errorf("failed acl lookup: %s", err)
	}
	return idx, acls, nil
}

func (s *Store) aclListTxn(tx *memdb.Txn, ws memdb.WatchSet) (structs.ACLs, error) {

	iter, err := tx.Get("acls", "id")
	if err != nil {
		return nil, fmt.Errorf("failed acl lookup: %s", err)
	}
	ws.Add(iter.WatchCh())

	var result structs.ACLs
	for acl := iter.Next(); acl != nil; acl = iter.Next() {
		a := acl.(*structs.ACL)
		result = append(result, a)
	}
	return result, nil
}

func (s *Store) ACLDelete(idx uint64, aclID string) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.aclDeleteTxn(tx, idx, aclID); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) aclDeleteTxn(tx *memdb.Txn, idx uint64, aclID string) error {

	acl, err := tx.First("acls", "id", aclID)
	if err != nil {
		return fmt.Errorf("failed acl lookup: %s", err)
	}
	if acl == nil {
		return nil
	}

	if err := tx.Delete("acls", acl); err != nil {
		return fmt.Errorf("failed deleting acl: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{"acls", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}
