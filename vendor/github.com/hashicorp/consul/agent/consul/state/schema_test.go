package state

import (
	"testing"

	"github.com/hashicorp/go-memdb"
)

func TestStateStore_Schema(t *testing.T) {

	schema := stateStoreSchema()

	if _, err := memdb.NewMemDB(schema); err != nil {
		t.Fatalf("err: %s", err)
	}
}
