package state

import (
	"fmt"

	"github.com/hashicorp/go-memdb"
)

type schemaFn func() *memdb.TableSchema

var schemas []schemaFn

func registerSchema(fn schemaFn) {
	schemas = append(schemas, fn)
}

func stateStoreSchema() *memdb.DBSchema {

	db := &memdb.DBSchema{
		Tables: make(map[string]*memdb.TableSchema),
	}

	for _, fn := range schemas {
		schema := fn()
		if _, ok := db.Tables[schema.Name]; ok {
			panic(fmt.Sprintf("duplicate table name: %s", schema.Name))
		}
		db.Tables[schema.Name] = schema
	}
	return db
}

func indexTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: "index",
		Indexes: map[string]*memdb.IndexSchema{
			"id": {
				Name:         "id",
				AllowMissing: false,
				Unique:       true,
				Indexer: &memdb.StringFieldIndex{
					Field:     "Key",
					Lowercase: true,
				},
			},
		},
	}
}

func init() {
	registerSchema(indexTableSchema)
}
