package builder

import (
	"github.com/dopsilva/rdd/engine"
	"github.com/dopsilva/rdd/field"
	"github.com/dopsilva/rdd/schema"
)

type CreateTableOptions struct {
	IfNotExists  bool
	DropIfExists bool
}

type Builder interface {
	CreateTable(*schema.Table, *CreateTableOptions) (string, error)
	Insert(schema.Table, []field.FieldInstance) (string, []any, []any, error)
	Update(schema.Table, []field.FieldInstance) (string, []any, []any, error)
	Delete(schema.Table, []field.FieldInstance) (string, []any)

	QuotedIdentifier(i string) string
	QuotedValue(v any) string
}

func New(e engine.Engine) Builder {
	switch e {
	case engine.SQLite:
		return SQLite{}
	}
	panic("rdd: unknown engine")
}
