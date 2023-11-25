package rdd

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Workarea[T any] interface {
	CreateTable(ctx context.Context, db Database, options *CreateTableOptions) error

	Append(ctx context.Context, db Database) error
	Replace(ctx context.Context, db Database) error
	Delete(ctx context.Context, db Database) error

	Changed() bool
	Load(src any) error
	Freeze()

	Event(params EventParameters) error
}

type workarea[T any] struct {
	entity *T
	table  string
	fields map[string]fieldSchema
	lastop Operation
}

type fieldSchema struct {
	name      string
	pk        bool
	uk        bool
	auto      bool
	nullable  bool
	def       string
	fieldaddr any
	fieldtype string
}

type EventType int

const (
	BeforeAppend EventType = iota
	AfterAppend
	BeforeReplace
	AfterReplace
	BeforeDelete
	AfterDelete
	AfterCommit
)

type Operation int

const (
	Append Operation = iota
	Replace
	Delete
)

type EventParameters struct {
	Context   context.Context
	Database  Database
	Type      EventType
	Operation Operation
}

func Use[T any]() (*T, error) {
	i := new(T)
	w, err := newWorkarea[T](i)
	if err != nil {
		return nil, err
	}

	// TODO: verificar se o field Workarea existe
	reflect.ValueOf(i).Elem().FieldByName("Workarea").Set(reflect.ValueOf(w))

	return i, nil
}

func newWorkarea[T any](entity *T) (Workarea[T], error) {
	w := &workarea[T]{
		entity: entity,
		fields: make(map[string]fieldSchema, 0),
	}

	rv := reflect.ValueOf(entity).Elem()
	rt := reflect.TypeOf(entity).Elem()

	for i := 0; i < rv.NumField(); i++ {
		f := rv.Field(i).Addr()

		switch v := f.Interface().(type) {
		case *Workarea[T]:
			if tv, ok := rt.Field(i).Tag.Lookup("rdd-table"); ok {
				w.table = tv
			} else {
				return nil, errors.New("workarea: rdd-table not defined")
			}
		default:
			if ti, ok := v.(Typed); ok {
				columnName, ok := rt.Field(i).Tag.Lookup("rdd-column")
				if ok {
					var pk, uk, auto, nullable bool
					var def string

					if tv, ok := rt.Field(i).Tag.Lookup("rdd-primary-key"); ok {
						pk, _ = strconv.ParseBool(tv)
					}
					if tv, ok := rt.Field(i).Tag.Lookup("rdd-unique-key"); ok {
						uk, _ = strconv.ParseBool(tv)
					}
					if tv, ok := rt.Field(i).Tag.Lookup("rdd-auto-generated"); ok {
						auto, _ = strconv.ParseBool(tv)
					}
					if tv, ok := rt.Field(i).Tag.Lookup("rdd-nullable"); ok {
						nullable, _ = strconv.ParseBool(tv)
					} else {
						nullable = true
					}
					if tv, ok := rt.Field(i).Tag.Lookup("rdd-default"); ok {
						def = tv
					}

					column := fieldSchema{
						name:      columnName,
						pk:        pk,
						uk:        uk,
						auto:      auto,
						nullable:  nullable,
						def:       def,
						fieldaddr: f.Interface(),
						fieldtype: ti.Type().Name(),
					}

					w.fields[columnName] = column
				}
			}
		}
	}

	return w, nil
}

type CreateTableOptions struct {
	IfNotExists bool
}

func (w *workarea[T]) CreateTable(ctx context.Context, db Database, options *CreateTableOptions) error {
	var b strings.Builder
	pk := make([]fieldSchema, 0)
	var uk *fieldSchema
	opt := CreateTableOptions{}

	if options != nil {
		opt = *options
	}

	b.WriteString("create table")

	if opt.IfNotExists {
		b.WriteString(" if not exists")
	}

	b.WriteString(" " + quotedIdentifier(db.Engine(), w.table) + " (")

	n := 0
	for _, f := range w.fields {
		if n > 0 {
			b.WriteString(", ")
		}
		b.WriteString(columnCreate(db, f))
		if f.pk {
			pk = append(pk, f)
		}
		if f.uk {
			uk = &f
		}
		n++
	}

	if len(pk) > 0 {
		b.WriteString(", primary key (")
		for i, f := range pk {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(quotedIdentifier(db.Engine(), f.name))
		}
		b.WriteString(")")
	}

	if uk != nil {
		b.WriteString(", unique (" + quotedIdentifier(db.Engine(), uk.name) + ")")
	}

	b.WriteString(");")
	//fmt.Println(b.String())

	if _, err := db.Exec(b.String()); err != nil {
		return err
	}

	return nil
}

func columnCreate(db Database, f fieldSchema) string {
	var b strings.Builder

	b.WriteString(quotedIdentifier(db.Engine(), f.name))

	//fmt.Println(f.fieldtype)

	switch f.fieldtype {
	case "string", "NullString":
		switch db.Engine() {
		case SQLite:
			b.WriteString(" text")
		}
	case "int", "int64", "NullInt64":
		switch db.Engine() {
		case SQLite:
			b.WriteString(" integer")
		}
	case "bool", "NullBool":
		switch db.Engine() {
		case SQLite:
			b.WriteString(" integer")
		}
	case "float64", "NullFloat64":
		switch db.Engine() {
		case SQLite:
			b.WriteString(" real")
		}
	case "Time", "NullTime":
		switch db.Engine() {
		case SQLite:
			b.WriteString(" text")
		}
	}

	if f.nullable {
		b.WriteString(" null")
	} else {
		b.WriteString(" not null")
	}

	if f.def != "" {
		b.WriteString(" default ")
		switch f.def {
		case "new_uuid":
			b.WriteString(db.Engine().DefaultRandomUUID())
		case "now":
			b.WriteString(db.Engine().DefaultCurrentTimestamp())
		}
	}

	return b.String()
}

func (w *workarea[T]) Append(ctx context.Context, db Database) error {
	// verifica se implementa o event handler
	handler, hasHandler := implements[Workarea[T]](w)

	// executa o event handler
	if hasHandler {
		if err := handler.Event(EventParameters{Type: BeforeAppend, Context: ctx, Database: db}); err != nil {
			return err
		}
	}

	// executa o insert
	query, args, ret, err := w.generateInsert(db.Engine())
	if err != nil {
		return err
	}

	//fmt.Println(query)

	if len(ret) == 0 {
		if _, err := db.Exec(query, args...); err != nil {
			return err
		}
	} else {
		if err := db.QueryRow(query, args...).Scan(ret...); err != nil {
			return err
		}
	}

	// executa o event handler
	if hasHandler {
		if err := handler.Event(EventParameters{Type: AfterAppend, Context: ctx, Database: db}); err != nil {
			return err
		}
	}

	w.lastop = Append

	if !db.WithinTransaction() {
		w.Freeze()
	} else {
		// TODO: armazenar a workarea para dar um freeze depois do commit da transação
	}

	return nil
}

func (w *workarea[T]) Replace(ctx context.Context, db Database) error {
	// verifica se implementa o event handler
	handler, hasHandler := implements[Workarea[T]](w)

	// executa o event handler
	if hasHandler {
		if err := handler.Event(EventParameters{Type: BeforeReplace, Context: ctx, Database: db}); err != nil {
			return err
		}
	}

	// executa o update
	query, args, ret, err := w.generateUpdate(db.Engine())
	if err != nil {
		return err
	}

	fmt.Println(query)

	if len(ret) == 0 {
		if _, err := db.Exec(query, args...); err != nil {
			return err
		}
	} else {
		if err := db.QueryRow(query, args...).Scan(ret...); err != nil {
			return err
		}
	}

	// executa o event handler
	if hasHandler {
		if err := handler.Event(EventParameters{Type: AfterReplace, Context: ctx, Database: db}); err != nil {
			return err
		}
	}

	w.lastop = Replace

	if !db.WithinTransaction() {
		w.Freeze()
	} else {
		// TODO: armazenar a workarea para dar um freeze depois do commit da transação
	}

	return nil
}

func (w *workarea[T]) Delete(ctx context.Context, db Database) error {
	w.lastop = Delete
	return nil
}

func (w workarea[T]) Changed() bool {
	for _, v := range w.fields {
		if f, ok := v.fieldaddr.(Changeable); ok {
			if f.Changed() {
				return true
			}
		}
	}
	return false
}

func (w workarea[T]) Freeze() {
	for _, v := range w.fields {
		if f, ok := v.fieldaddr.(Freezable); ok {
			f.Freeze()
		}
	}
}

func (w workarea[T]) Load(src any) error {
	rv := reflect.ValueOf(src)
	rt := reflect.TypeOf(src)

	for i := 0; i < rv.NumField(); i++ {
		sv := rv.Field(i).Interface()

		if columnName, ok := rt.Field(i).Tag.Lookup("rdd-column"); ok {
			if column, ok := w.fields[columnName]; ok {
				switch v := sv.(type) {
				case string:
					(column.fieldaddr.(*Field[string])).Set(v)
				case int:
					(column.fieldaddr.(*Field[int])).Set(v)
				case int64:
					(column.fieldaddr.(*Field[int64])).Set(v)
				case float64:
					(column.fieldaddr.(*Field[float64])).Set(v)
				case bool:
					(column.fieldaddr.(*Field[bool])).Set(v)
				case time.Time:
					(column.fieldaddr.(*Field[time.Time])).Set(v)
				}
			}
		}
	}

	return nil
}

func (w workarea[T]) Event(params EventParameters) error {
	return nil
}

func implements[I, T any](w *workarea[T]) (I, bool) {
	c, ok := any(w.entity).(I)
	return c, ok
}

func (w workarea[T]) generateInsert(eng DatabaseEngine) (string, []any, []any, error) {
	var q strings.Builder
	arguments := make([]any, 0)
	retfields := make([]fieldSchema, 0)
	returning := make([]any, 0)

	q.WriteString("insert into " + eng.QuotedIdentifier(w.table) + " (")

	n := 0
	for k, v := range w.fields {
		if v.auto {
			retfields = append(retfields, v)
			returning = append(returning, v.fieldaddr)
			continue
		}
		if n > 0 {
			q.WriteString(", ")
		}
		q.WriteString(eng.QuotedIdentifier(k))
		arguments = append(arguments, v.fieldaddr)
		n++
	}

	q.WriteString(") values (")

	for i := range arguments {
		if i > 0 {
			q.WriteString(", ")
		}
		q.WriteString(fmt.Sprintf("$%d", i+1))
	}

	q.WriteString(")")

	if len(retfields) > 0 {
		q.WriteString(" returning ")

		n = 0
		for _, v := range retfields {
			if n > 0 {
				q.WriteString(", ")
			}
			q.WriteString(eng.QuotedIdentifier(v.name))
			n += 1
		}
	}

	q.WriteString(";")

	return q.String(), arguments, returning, nil
}

func (w workarea[T]) generateUpdate(eng DatabaseEngine) (string, []any, []any, error) {
	var q strings.Builder
	arguments := make([]any, 0)
	retfields := make([]fieldSchema, 0)
	returning := make([]any, 0)
	pk := make([]fieldSchema, 0)

	q.WriteString("update " + eng.QuotedIdentifier(w.table) + " set ")

	n := 0
	i := 1
	for k, v := range w.fields {
		if v.pk {
			pk = append(pk, v)
		}
		if v.auto {
			retfields = append(retfields, v)
			returning = append(returning, v.fieldaddr)
			continue
		}
		if c, ok := v.fieldaddr.(Changeable); ok {
			if !c.Changed() {
				continue
			}
		} else if !ok {
			continue
		}
		if n > 0 {
			q.WriteString(", ")
		}
		q.WriteString(eng.QuotedIdentifier(k) + " = " + fmt.Sprintf("$%d", i))
		arguments = append(arguments, v.fieldaddr)
		n++
		i++
	}

	if len(retfields) > 0 {
		q.WriteString(" returning ")

		n = 0
		for _, v := range retfields {
			if n > 0 {
				q.WriteString(", ")
			}
			q.WriteString(eng.QuotedIdentifier(v.name))
			n += 1
		}
	}

	q.WriteString(" where ")

	if where, wargs, ok := w.wherePrimaryKey(eng, len(arguments)); ok {
		q.WriteString(where)
		arguments = append(arguments, wargs...)
	} else if where, wargs, ok = w.whereUniqueKey(eng, len(arguments)); ok {
		q.WriteString(where)
		arguments = append(arguments, wargs...)
	} else {
		panic("tabela sem primary ou unique key definido")
	}

	q.WriteString(";")

	return q.String(), arguments, returning, nil
}

func (w workarea[T]) wherePrimaryKey(eng DatabaseEngine, argsCount int) (string, []any, bool) {
	var sb strings.Builder
	args := make([]any, 0)
	haspk := false

	i := 0
	for _, v := range w.fields {
		if v.pk {
			if i > 0 {
				sb.WriteString(" and ")
			}
			sb.WriteString(eng.QuotedIdentifier(v.name) + " = " + fmt.Sprintf("$%d", argsCount+i+1))
			args = append(args, v.fieldaddr)
			haspk = true
		}
	}

	return sb.String(), args, haspk
}

func (w workarea[T]) whereUniqueKey(eng DatabaseEngine, argsCount int) (string, []any, bool) {
	var sb strings.Builder
	args := make([]any, 0)
	hasuk := false

	i := 0
	for _, v := range w.fields {
		if v.uk {
			if i > 0 {
				sb.WriteString(" and ")
			}
			sb.WriteString(eng.QuotedIdentifier(v.name) + " = " + fmt.Sprintf("$%d", argsCount+i+1))
			args = append(args, v.fieldaddr)
			hasuk = true
		}
	}

	return sb.String(), args, hasuk
}

func quotedIdentifier(eng DatabaseEngine, v string) string {
	return fmt.Sprintf("\"%s\"", v)
}
