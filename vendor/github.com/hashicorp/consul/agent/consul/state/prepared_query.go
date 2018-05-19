package state

import (
	"fmt"
	"regexp"

	"github.com/hashicorp/consul/agent/consul/prepared_query"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/go-memdb"
)

func preparedQueriesTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "prepared-queries",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.UUIDFieldIndex{
					Field: "ID",
				},
			},
			"name": {
				Name:         "name",
				AllowMissing: true,
				Unique:       true,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Name",
					Lowercase: true,
				},
			},
			"template": {
				Name:         "template",
				AllowMissing: true,
				Unique:       true,
				Indexer:      &PreparedQueryIndex{},
			},
			"session": {
				Name:         "session",
				AllowMissing: true,
				Unique:       false,
				Indexer: &memdb.UUIDFieldIndex{
					Field: "Session",
				},
			},
		},
	}
}

func init() {
	registerSchema(preparedQueriesTableSchema)
}

var validUUID = regexp.MustCompile(`(?i)^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$`)

func isUUID(str string) bool {
	const uuidLen = 36
	if len(str) != uuidLen {
		return false
	}

	return validUUID.MatchString(str)
}

type queryWrapper struct {
	*structs.PreparedQuery

	ct *prepared_query.CompiledTemplate
}

func toPreparedQuery(wrapped interface{}) *structs.PreparedQuery {
	if wrapped == nil {
		return nil
	}
	return wrapped.(*queryWrapper).PreparedQuery
}

func (s *Snapshot) PreparedQueries() (structs.PreparedQueries, error) {
	queries, err := s.tx.Get("prepared-queries", "id")
	if err != nil {
		return nil, err
	}

	var ret structs.PreparedQueries
	for wrapped := queries.Next(); wrapped != nil; wrapped = queries.Next() {
		ret = append(ret, toPreparedQuery(wrapped))
	}
	return ret, nil
}

func (s *Restore) PreparedQuery(query *structs.PreparedQuery) error {

	var ct *prepared_query.CompiledTemplate
	if prepared_query.IsTemplate(query) {
		var err error
		ct, err = prepared_query.Compile(query)
		if err != nil {
			return fmt.Errorf("failed compiling template: %s", err)
		}
	}

	if err := s.tx.Insert("prepared-queries", &queryWrapper{query, ct}); err != nil {
		return fmt.Errorf("failed restoring prepared query: %s", err)
	}
	if err := indexUpdateMaxTxn(s.tx, query.ModifyIndex, "prepared-queries"); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

func (s *Store) PreparedQuerySet(idx uint64, query *structs.PreparedQuery) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.preparedQuerySetTxn(tx, idx, query); err != nil {
		return err
	}

	tx.Commit()
	return nil
}

func (s *Store) preparedQuerySetTxn(tx *memdb.Txn, idx uint64, query *structs.PreparedQuery) error {

	if query.ID == "" {
		return ErrMissingQueryID
	}

	wrapped, err := tx.First("prepared-queries", "id", query.ID)
	if err != nil {
		return fmt.Errorf("failed prepared query lookup: %s", err)
	}
	existing := toPreparedQuery(wrapped)

	if existing != nil {
		query.CreateIndex = existing.CreateIndex
		query.ModifyIndex = idx
	} else {
		query.CreateIndex = idx
		query.ModifyIndex = idx
	}

	if query.Name != "" {
		wrapped, err := tx.First("prepared-queries", "name", query.Name)
		if err != nil {
			return fmt.Errorf("failed prepared query lookup: %s", err)
		}
		other := toPreparedQuery(wrapped)
		if other != nil && (existing == nil || existing.ID != other.ID) {
			return fmt.Errorf("name '%s' aliases an existing query name", query.Name)
		}
	} else if prepared_query.IsTemplate(query) {
		wrapped, err := tx.First("prepared-queries", "template", query.Name)
		if err != nil {
			return fmt.Errorf("failed prepared query lookup: %s", err)
		}
		other := toPreparedQuery(wrapped)
		if other != nil && (existing == nil || existing.ID != other.ID) {
			return fmt.Errorf("a query template with an empty name already exists")
		}
	}

	if isUUID(query.Name) {
		wrapped, err := tx.First("prepared-queries", "id", query.Name)
		if err != nil {
			return fmt.Errorf("failed prepared query lookup: %s", err)
		}
		if wrapped != nil {
			return fmt.Errorf("name '%s' aliases an existing query ID", query.Name)
		}
	}

	if query.Session != "" {
		sess, err := tx.First("sessions", "id", query.Session)
		if err != nil {
			return fmt.Errorf("failed session lookup: %s", err)
		}
		if sess == nil {
			return fmt.Errorf("invalid session %#v", query.Session)
		}
	}

	var ct *prepared_query.CompiledTemplate
	if prepared_query.IsTemplate(query) {
		var err error
		ct, err = prepared_query.Compile(query)
		if err != nil {
			return fmt.Errorf("failed compiling template: %s", err)
		}
	}

	if err := tx.Insert("prepared-queries", &queryWrapper{query, ct}); err != nil {
		return fmt.Errorf("failed inserting prepared query: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{"prepared-queries", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

func (s *Store) PreparedQueryDelete(idx uint64, queryID string) error {
	tx := s.db.Txn(true)
	defer tx.Abort()

	if err := s.preparedQueryDeleteTxn(tx, idx, queryID); err != nil {
		return fmt.Errorf("failed prepared query delete: %s", err)
	}

	tx.Commit()
	return nil
}

func (s *Store) preparedQueryDeleteTxn(tx *memdb.Txn, idx uint64, queryID string) error {

	wrapped, err := tx.First("prepared-queries", "id", queryID)
	if err != nil {
		return fmt.Errorf("failed prepared query lookup: %s", err)
	}
	if wrapped == nil {
		return nil
	}

	if err := tx.Delete("prepared-queries", wrapped); err != nil {
		return fmt.Errorf("failed prepared query delete: %s", err)
	}
	if err := tx.Insert("index", &IndexEntry{"prepared-queries", idx}); err != nil {
		return fmt.Errorf("failed updating index: %s", err)
	}

	return nil
}

func (s *Store) PreparedQueryGet(ws memdb.WatchSet, queryID string) (uint64, *structs.PreparedQuery, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "prepared-queries")

	watchCh, wrapped, err := tx.FirstWatch("prepared-queries", "id", queryID)
	if err != nil {
		return 0, nil, fmt.Errorf("failed prepared query lookup: %s", err)
	}
	ws.Add(watchCh)
	return idx, toPreparedQuery(wrapped), nil
}

func (s *Store) PreparedQueryResolve(queryIDOrName string, source structs.QuerySource) (uint64, *structs.PreparedQuery, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "prepared-queries")

	if queryIDOrName == "" {
		return 0, nil, ErrMissingQueryID
	}

	if isUUID(queryIDOrName) {
		wrapped, err := tx.First("prepared-queries", "id", queryIDOrName)
		if err != nil {
			return 0, nil, fmt.Errorf("failed prepared query lookup: %s", err)
		}
		if wrapped != nil {
			query := toPreparedQuery(wrapped)
			if prepared_query.IsTemplate(query) {
				return idx, nil, fmt.Errorf("prepared query templates can only be resolved up by name, not by ID")
			}
			return idx, query, nil
		}
	}

	prep := func(wrapped interface{}) (uint64, *structs.PreparedQuery, error) {
		wrapper := wrapped.(*queryWrapper)
		if prepared_query.IsTemplate(wrapper.PreparedQuery) {
			render, err := wrapper.ct.Render(queryIDOrName, source)
			if err != nil {
				return idx, nil, err
			}
			return idx, render, nil
		}
		return idx, wrapper.PreparedQuery, nil
	}

	{
		wrapped, err := tx.First("prepared-queries", "name", queryIDOrName)
		if err != nil {
			return 0, nil, fmt.Errorf("failed prepared query lookup: %s", err)
		}
		if wrapped != nil {
			return prep(wrapped)
		}
	}

	{
		wrapped, err := tx.LongestPrefix("prepared-queries", "template_prefix", queryIDOrName)
		if err != nil {
			return 0, nil, fmt.Errorf("failed prepared query lookup: %s", err)
		}
		if wrapped != nil {
			return prep(wrapped)
		}
	}

	return idx, nil, nil
}

func (s *Store) PreparedQueryList(ws memdb.WatchSet) (uint64, structs.PreparedQueries, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()

	idx := maxIndexTxn(tx, "prepared-queries")

	queries, err := tx.Get("prepared-queries", "id")
	if err != nil {
		return 0, nil, fmt.Errorf("failed prepared query lookup: %s", err)
	}
	ws.Add(queries.WatchCh())

	var result structs.PreparedQueries
	for wrapped := queries.Next(); wrapped != nil; wrapped = queries.Next() {
		result = append(result, toPreparedQuery(wrapped))
	}
	return idx, result, nil
}
