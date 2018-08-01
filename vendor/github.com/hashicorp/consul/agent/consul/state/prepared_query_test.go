package state

import (
	"reflect"
	"strings"
	"testing"

	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/go-memdb"
)

func TestStateStore_PreparedQuery_isUUID(t *testing.T) {
	cases := map[string]bool{
		"":     false,
		"nope": false,
		"f004177f-2c28-83b7-4229-eacc25fe55d1":  true,
		"F004177F-2C28-83B7-4229-EACC25FE55D1":  true,
		"x004177f-2c28-83b7-4229-eacc25fe55d1":  false, // Bad hex
		"f004177f-xc28-83b7-4229-eacc25fe55d1":  false, // Bad hex
		"f004177f-2c28-x3b7-4229-eacc25fe55d1":  false, // Bad hex
		"f004177f-2c28-83b7-x229-eacc25fe55d1":  false, // Bad hex
		"f004177f-2c28-83b7-4229-xacc25fe55d1":  false, // Bad hex
		" f004177f-2c28-83b7-4229-eacc25fe55d1": false, // Leading whitespace
		"f004177f-2c28-83b7-4229-eacc25fe55d1 ": false, // Trailing whitespace
	}
	for i := 0; i < 100; i++ {
		cases[testUUID()] = true
	}

	for str, expected := range cases {
		if actual := isUUID(str); actual != expected {
			t.Fatalf("bad: '%s'", str)
		}
	}
}

func TestStateStore_PreparedQuerySet_PreparedQueryGet(t *testing.T) {
	s := testStateStore(t)

	ws := memdb.NewWatchSet()
	idx, res, err := s.PreparedQueryGet(ws, testUUID())
	if idx != 0 || res != nil || err != nil {
		t.Fatalf("expected (0, nil, nil), got: (%d, %#v, %#v)", idx, res, err)
	}

	if err := s.PreparedQuerySet(1, &structs.PreparedQuery{}); err == nil {
		t.Fatalf("expected %#v, got: %#v", ErrMissingQueryID, err)
	}

	if idx := s.maxIndex("prepared-queries"); idx != 0 {
		t.Fatalf("bad index: %d", idx)
	}
	if watchFired(ws) {
		t.Fatalf("bad")
	}

	query := &structs.PreparedQuery{
		ID:      testUUID(),
		Session: "nope",
		Service: structs.ServiceQuery{
			Service: "redis",
		},
	}

	err = s.PreparedQuerySet(1, query)
	if err == nil || !strings.Contains(err.Error(), "failed session lookup") {
		t.Fatalf("bad: %v", err)
	}

	if idx := s.maxIndex("prepared-queries"); idx != 0 {
		t.Fatalf("bad index: %d", idx)
	}
	if watchFired(ws) {
		t.Fatalf("bad")
	}

	testRegisterNode(t, s, 1, "foo")
	testRegisterService(t, s, 2, "foo", "redis")
	query.Session = ""

	if err := s.PreparedQuerySet(3, query); err != nil {
		t.Fatalf("err: %s", err)
	}

	if idx := s.maxIndex("prepared-queries"); idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}

	expected := &structs.PreparedQuery{
		ID: query.ID,
		Service: structs.ServiceQuery{
			Service: "redis",
		},
		RaftIndex: structs.RaftIndex{
			CreateIndex: 3,
			ModifyIndex: 3,
		},
	}
	ws = memdb.NewWatchSet()
	idx, actual, err := s.PreparedQueryGet(ws, query.ID)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %v", actual)
	}

	query.Name = "test-query"
	if err := s.PreparedQuerySet(4, query); err != nil {
		t.Fatalf("err: %s", err)
	}

	if idx := s.maxIndex("prepared-queries"); idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}

	expected.Name = "test-query"
	expected.ModifyIndex = 4
	ws = memdb.NewWatchSet()
	idx, actual, err = s.PreparedQueryGet(ws, query.ID)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %v", actual)
	}

	query.Session = testUUID()
	err = s.PreparedQuerySet(5, query)
	if err == nil || !strings.Contains(err.Error(), "invalid session") {
		t.Fatalf("bad: %v", err)
	}

	if idx := s.maxIndex("prepared-queries"); idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}
	if watchFired(ws) {
		t.Fatalf("bad")
	}

	session := &structs.Session{
		ID:   query.Session,
		Node: "foo",
	}
	if err := s.SessionCreate(5, session); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := s.PreparedQuerySet(6, query); err != nil {
		t.Fatalf("err: %s", err)
	}

	if idx := s.maxIndex("prepared-queries"); idx != 6 {
		t.Fatalf("bad index: %d", idx)
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}

	expected.Session = query.Session
	expected.ModifyIndex = 6
	ws = memdb.NewWatchSet()
	idx, actual, err = s.PreparedQueryGet(ws, query.ID)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 6 {
		t.Fatalf("bad index: %d", idx)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %v", actual)
	}

	{
		evil := &structs.PreparedQuery{
			ID:   testUUID(),
			Name: query.Name,
			Service: structs.ServiceQuery{
				Service: "redis",
			},
		}
		err := s.PreparedQuerySet(7, evil)
		if err == nil || !strings.Contains(err.Error(), "aliases an existing query name") {
			t.Fatalf("bad: %v", err)
		}

		idx, actual, err := s.PreparedQueryGet(nil, evil.ID)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if idx != 6 {
			t.Fatalf("bad index: %d", idx)
		}
		if actual != nil {
			t.Fatalf("bad: %v", actual)
		}
	}

	{
		evil := &structs.PreparedQuery{
			ID:   testUUID(),
			Name: query.ID,
			Service: structs.ServiceQuery{
				Service: "redis",
			},
		}
		err := s.PreparedQuerySet(8, evil)
		if err == nil || !strings.Contains(err.Error(), "aliases an existing query ID") {
			t.Fatalf("bad: %v", err)
		}

		idx, actual, err := s.PreparedQueryGet(nil, evil.ID)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if idx != 6 {
			t.Fatalf("bad index: %d", idx)
		}
		if actual != nil {
			t.Fatalf("bad: %v", actual)
		}
	}

	{
		evil := &structs.PreparedQuery{
			ID:   testUUID(),
			Name: query.Name,
			Template: structs.QueryTemplateOptions{
				Type: structs.QueryTemplateTypeNamePrefixMatch,
			},
			Service: structs.ServiceQuery{
				Service: "redis",
			},
		}
		err := s.PreparedQuerySet(8, evil)
		if err == nil || !strings.Contains(err.Error(), "aliases an existing query name") {
			t.Fatalf("bad: %v", err)
		}

		idx, actual, err := s.PreparedQueryGet(nil, evil.ID)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if idx != 6 {
			t.Fatalf("bad index: %d", idx)
		}
		if actual != nil {
			t.Fatalf("bad: %v", actual)
		}
	}

	if idx := s.maxIndex("prepared-queries"); idx != 6 {
		t.Fatalf("bad index: %d", idx)
	}
	if watchFired(ws) {
		t.Fatalf("bad")
	}

	query.Name = ""
	query.Template = structs.QueryTemplateOptions{
		Type: structs.QueryTemplateTypeNamePrefixMatch,
	}
	if err := s.PreparedQuerySet(9, query); err != nil {
		t.Fatalf("err: %s", err)
	}

	if idx := s.maxIndex("prepared-queries"); idx != 9 {
		t.Fatalf("bad index: %d", idx)
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}

	expected.Name = ""
	expected.Template = structs.QueryTemplateOptions{
		Type: structs.QueryTemplateTypeNamePrefixMatch,
	}
	expected.ModifyIndex = 9
	ws = memdb.NewWatchSet()
	idx, actual, err = s.PreparedQueryGet(ws, query.ID)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 9 {
		t.Fatalf("bad index: %d", idx)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %v", actual)
	}

	{
		evil := &structs.PreparedQuery{
			ID:   testUUID(),
			Name: "",
			Template: structs.QueryTemplateOptions{
				Type: structs.QueryTemplateTypeNamePrefixMatch,
			},
			Service: structs.ServiceQuery{
				Service: "redis",
			},
		}
		err := s.PreparedQuerySet(10, evil)
		if err == nil || !strings.Contains(err.Error(), "query template with an empty name already exists") {
			t.Fatalf("bad: %v", err)
		}

		idx, actual, err := s.PreparedQueryGet(nil, evil.ID)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if idx != 9 {
			t.Fatalf("bad index: %d", idx)
		}
		if actual != nil {
			t.Fatalf("bad: %v", actual)
		}
	}

	query.Name = "prefix"
	if err := s.PreparedQuerySet(11, query); err != nil {
		t.Fatalf("err: %s", err)
	}

	if idx := s.maxIndex("prepared-queries"); idx != 11 {
		t.Fatalf("bad index: %d", idx)
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}

	expected.Name = "prefix"
	expected.ModifyIndex = 11
	ws = memdb.NewWatchSet()
	idx, actual, err = s.PreparedQueryGet(ws, query.ID)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 11 {
		t.Fatalf("bad index: %d", idx)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %v", actual)
	}

	{
		evil := &structs.PreparedQuery{
			ID:   testUUID(),
			Name: "prefix",
			Template: structs.QueryTemplateOptions{
				Type: structs.QueryTemplateTypeNamePrefixMatch,
			},
			Service: structs.ServiceQuery{
				Service: "redis",
			},
		}
		err := s.PreparedQuerySet(12, evil)
		if err == nil || !strings.Contains(err.Error(), "aliases an existing query name") {
			t.Fatalf("bad: %v", err)
		}

		idx, actual, err := s.PreparedQueryGet(nil, evil.ID)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if idx != 11 {
			t.Fatalf("bad index: %d", idx)
		}
		if actual != nil {
			t.Fatalf("bad: %v", actual)
		}
	}

	{
		evil := &structs.PreparedQuery{
			ID:   testUUID(),
			Name: "legit-prefix",
			Template: structs.QueryTemplateOptions{
				Type: structs.QueryTemplateTypeNamePrefixMatch,
			},
			Service: structs.ServiceQuery{
				Service: "${nope",
			},
		}
		err := s.PreparedQuerySet(13, evil)
		if err == nil || !strings.Contains(err.Error(), "failed compiling template") {
			t.Fatalf("bad: %v", err)
		}

		idx, actual, err := s.PreparedQueryGet(nil, evil.ID)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if idx != 11 {
			t.Fatalf("bad index: %d", idx)
		}
		if actual != nil {
			t.Fatalf("bad: %v", actual)
		}
	}

	if watchFired(ws) {
		t.Fatalf("bad")
	}
}

func TestStateStore_PreparedQueryDelete(t *testing.T) {
	s := testStateStore(t)

	testRegisterNode(t, s, 1, "foo")
	testRegisterService(t, s, 2, "foo", "redis")

	query := &structs.PreparedQuery{
		ID: testUUID(),
		Service: structs.ServiceQuery{
			Service: "redis",
		},
	}

	if err := s.PreparedQueryDelete(3, query.ID); err != nil {
		t.Fatalf("err: %s", err)
	}

	if idx := s.maxIndex("prepared-queries"); idx != 0 {
		t.Fatalf("bad index: %d", idx)
	}

	if err := s.PreparedQuerySet(3, query); err != nil {
		t.Fatalf("err: %s", err)
	}

	if idx := s.maxIndex("prepared-queries"); idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}

	expected := &structs.PreparedQuery{
		ID: query.ID,
		Service: structs.ServiceQuery{
			Service: "redis",
		},
		RaftIndex: structs.RaftIndex{
			CreateIndex: 3,
			ModifyIndex: 3,
		},
	}
	ws := memdb.NewWatchSet()
	idx, actual, err := s.PreparedQueryGet(ws, query.ID)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %v", actual)
	}

	if err := s.PreparedQueryDelete(4, query.ID); err != nil {
		t.Fatalf("err: %s", err)
	}

	if idx := s.maxIndex("prepared-queries"); idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}

	idx, actual, err = s.PreparedQueryGet(nil, query.ID)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 4 {
		t.Fatalf("bad index: %d", idx)
	}
	if actual != nil {
		t.Fatalf("bad: %v", actual)
	}
}

func TestStateStore_PreparedQueryResolve(t *testing.T) {
	s := testStateStore(t)

	testRegisterNode(t, s, 1, "foo")
	testRegisterService(t, s, 2, "foo", "redis")

	query := &structs.PreparedQuery{
		ID:   testUUID(),
		Name: "my-test-query",
		Service: structs.ServiceQuery{
			Service: "redis",
		},
	}

	idx, actual, err := s.PreparedQueryResolve(query.ID, structs.QuerySource{})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad index: %d", idx)
	}
	if actual != nil {
		t.Fatalf("bad: %v", actual)
	}

	idx, actual, err = s.PreparedQueryResolve(query.Name, structs.QuerySource{})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad index: %d", idx)
	}
	if actual != nil {
		t.Fatalf("bad: %v", actual)
	}

	if err := s.PreparedQuerySet(3, query); err != nil {
		t.Fatalf("err: %s", err)
	}

	if idx := s.maxIndex("prepared-queries"); idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}

	expected := &structs.PreparedQuery{
		ID:   query.ID,
		Name: "my-test-query",
		Service: structs.ServiceQuery{
			Service: "redis",
		},
		RaftIndex: structs.RaftIndex{
			CreateIndex: 3,
			ModifyIndex: 3,
		},
	}
	idx, actual, err = s.PreparedQueryResolve(query.ID, structs.QuerySource{})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %v", actual)
	}

	idx, actual, err = s.PreparedQueryResolve(query.Name, structs.QuerySource{})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad index: %d", idx)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %v", actual)
	}

	idx, actual, err = s.PreparedQueryResolve("", structs.QuerySource{})
	if err != ErrMissingQueryID {
		t.Fatalf("bad: %v ", err)
	}
	if idx != 0 {
		t.Fatalf("bad index: %d", idx)
	}
	if actual != nil {
		t.Fatalf("bad: %v", actual)
	}

	tmpl1 := &structs.PreparedQuery{
		ID:   testUUID(),
		Name: "prod-",
		Template: structs.QueryTemplateOptions{
			Type: structs.QueryTemplateTypeNamePrefixMatch,
		},
		Service: structs.ServiceQuery{
			Service: "${name.suffix}",
		},
	}
	if err := s.PreparedQuerySet(4, tmpl1); err != nil {
		t.Fatalf("err: %s", err)
	}
	tmpl2 := &structs.PreparedQuery{
		ID:   testUUID(),
		Name: "prod-redis",
		Template: structs.QueryTemplateOptions{
			Type:   structs.QueryTemplateTypeNamePrefixMatch,
			Regexp: "^prod-(.*)$",
		},
		Service: structs.ServiceQuery{
			Service: "${match(1)}-master",
		},
	}
	if err := s.PreparedQuerySet(5, tmpl2); err != nil {
		t.Fatalf("err: %s", err)
	}

	expected = &structs.PreparedQuery{
		ID:   tmpl1.ID,
		Name: "prod-",
		Template: structs.QueryTemplateOptions{
			Type: structs.QueryTemplateTypeNamePrefixMatch,
		},
		Service: structs.ServiceQuery{
			Service: "mongodb",
		},
		RaftIndex: structs.RaftIndex{
			CreateIndex: 4,
			ModifyIndex: 4,
		},
	}
	idx, actual, err = s.PreparedQueryResolve("prod-mongodb", structs.QuerySource{})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 5 {
		t.Fatalf("bad index: %d", idx)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %v", actual)
	}

	expected = &structs.PreparedQuery{
		ID:   tmpl2.ID,
		Name: "prod-redis",
		Template: structs.QueryTemplateOptions{
			Type:   structs.QueryTemplateTypeNamePrefixMatch,
			Regexp: "^prod-(.*)$",
		},
		Service: structs.ServiceQuery{
			Service: "redis-foobar-master",
		},
		RaftIndex: structs.RaftIndex{
			CreateIndex: 5,
			ModifyIndex: 5,
		},
	}
	idx, actual, err = s.PreparedQueryResolve("prod-redis-foobar", structs.QuerySource{})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 5 {
		t.Fatalf("bad index: %d", idx)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %v", actual)
	}

	expected = &structs.PreparedQuery{
		ID:   tmpl1.ID,
		Name: "prod-",
		Template: structs.QueryTemplateOptions{
			Type: structs.QueryTemplateTypeNamePrefixMatch,
		},
		Service: structs.ServiceQuery{
			Service: "",
		},
		RaftIndex: structs.RaftIndex{
			CreateIndex: 4,
			ModifyIndex: 4,
		},
	}
	idx, actual, err = s.PreparedQueryResolve("prod-", structs.QuerySource{})
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 5 {
		t.Fatalf("bad index: %d", idx)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %v", actual)
	}

	_, _, err = s.PreparedQueryResolve(tmpl1.ID, structs.QuerySource{})
	if err == nil || !strings.Contains(err.Error(), "prepared query templates can only be resolved up by name") {
		t.Fatalf("bad: %v", err)
	}
}

func TestStateStore_PreparedQueryList(t *testing.T) {
	s := testStateStore(t)

	ws := memdb.NewWatchSet()
	idx, actual, err := s.PreparedQueryList(ws)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad index: %d", idx)
	}
	if len(actual) != 0 {
		t.Fatalf("bad: %v", actual)
	}

	testRegisterNode(t, s, 1, "foo")
	testRegisterService(t, s, 2, "foo", "redis")
	testRegisterService(t, s, 3, "foo", "mongodb")

	queries := structs.PreparedQueries{
		&structs.PreparedQuery{
			ID:   testUUID(),
			Name: "alice",
			Service: structs.ServiceQuery{
				Service: "redis",
			},
		},
		&structs.PreparedQuery{
			ID:   testUUID(),
			Name: "bob",
			Service: structs.ServiceQuery{
				Service: "mongodb",
			},
		},
	}

	queries[0].ID = "a" + queries[0].ID[1:]
	queries[1].ID = "b" + queries[1].ID[1:]

	for i, query := range queries {
		if err := s.PreparedQuerySet(uint64(4+i), query); err != nil {
			t.Fatalf("err: %s", err)
		}
	}
	if !watchFired(ws) {
		t.Fatalf("bad")
	}

	expected := structs.PreparedQueries{
		&structs.PreparedQuery{
			ID:   queries[0].ID,
			Name: "alice",
			Service: structs.ServiceQuery{
				Service: "redis",
			},
			RaftIndex: structs.RaftIndex{
				CreateIndex: 4,
				ModifyIndex: 4,
			},
		},
		&structs.PreparedQuery{
			ID:   queries[1].ID,
			Name: "bob",
			Service: structs.ServiceQuery{
				Service: "mongodb",
			},
			RaftIndex: structs.RaftIndex{
				CreateIndex: 5,
				ModifyIndex: 5,
			},
		},
	}
	idx, actual, err = s.PreparedQueryList(nil)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 5 {
		t.Fatalf("bad index: %d", idx)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %v", actual)
	}
}

func TestStateStore_PreparedQuery_Snapshot_Restore(t *testing.T) {
	s := testStateStore(t)

	testRegisterNode(t, s, 1, "foo")
	testRegisterService(t, s, 2, "foo", "redis")
	testRegisterService(t, s, 3, "foo", "mongodb")

	queries := structs.PreparedQueries{
		&structs.PreparedQuery{
			ID:   testUUID(),
			Name: "alice",
			Service: structs.ServiceQuery{
				Service: "redis",
			},
		},
		&structs.PreparedQuery{
			ID:   testUUID(),
			Name: "bob-",
			Template: structs.QueryTemplateOptions{
				Type: structs.QueryTemplateTypeNamePrefixMatch,
			},
			Service: structs.ServiceQuery{
				Service: "${name.suffix}",
			},
		},
	}

	queries[0].ID = "a" + queries[0].ID[1:]
	queries[1].ID = "b" + queries[1].ID[1:]

	for i, query := range queries {
		if err := s.PreparedQuerySet(uint64(4+i), query); err != nil {
			t.Fatalf("err: %s", err)
		}
	}

	snap := s.Snapshot()
	defer snap.Close()

	if err := s.PreparedQueryDelete(6, queries[0].ID); err != nil {
		t.Fatalf("err: %s", err)
	}

	if idx := snap.LastIndex(); idx != 5 {
		t.Fatalf("bad index: %d", idx)
	}
	expected := structs.PreparedQueries{
		&structs.PreparedQuery{
			ID:   queries[0].ID,
			Name: "alice",
			Service: structs.ServiceQuery{
				Service: "redis",
			},
			RaftIndex: structs.RaftIndex{
				CreateIndex: 4,
				ModifyIndex: 4,
			},
		},
		&structs.PreparedQuery{
			ID:   queries[1].ID,
			Name: "bob-",
			Template: structs.QueryTemplateOptions{
				Type: structs.QueryTemplateTypeNamePrefixMatch,
			},
			Service: structs.ServiceQuery{
				Service: "${name.suffix}",
			},
			RaftIndex: structs.RaftIndex{
				CreateIndex: 5,
				ModifyIndex: 5,
			},
		},
	}
	dump, err := snap.PreparedQueries()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(dump, expected) {
		t.Fatalf("bad: %v", dump)
	}

	func() {
		s := testStateStore(t)
		restore := s.Restore()
		for _, query := range dump {
			if err := restore.PreparedQuery(query); err != nil {
				t.Fatalf("err: %s", err)
			}
		}
		restore.Commit()

		idx, actual, err := s.PreparedQueryList(nil)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if idx != 5 {
			t.Fatalf("bad index: %d", idx)
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("bad: %v", actual)
		}

		_, query, err := s.PreparedQueryResolve("bob-backwards-is-bob", structs.QuerySource{})
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		if query == nil {
			t.Fatalf("should have resolved the query")
		}
		if query.Service.Service != "backwards-is-bob" {
			t.Fatalf("bad: %s", query.Service.Service)
		}
	}()
}
