package rdd

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"time"
)

type Workarea[T any] interface {
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
}

type fieldSchema struct {
	name      string
	pk        bool
	uk        bool
	auto      bool
	fieldaddr any
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

type EventParameters struct {
	Context  context.Context
	Database Database
	Type     EventType
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
			if _, ok := v.(Typed); ok {
				columnName, ok := rt.Field(i).Tag.Lookup("rdd-column")
				if ok {
					var pk, uk, auto bool

					if tv, ok := rt.Field(i).Tag.Lookup("rdd-primary-key"); ok {
						pk, _ = strconv.ParseBool(tv)
					}
					if tv, ok := rt.Field(i).Tag.Lookup("rdd-unique-key"); ok {
						uk, _ = strconv.ParseBool(tv)
					}
					if tv, ok := rt.Field(i).Tag.Lookup("rdd-auto-generated"); ok {
						auto, _ = strconv.ParseBool(tv)
					}

					column := fieldSchema{
						name:      columnName,
						pk:        pk,
						uk:        uk,
						auto:      auto,
						fieldaddr: f.Interface(),
					}

					w.fields[columnName] = column
				}
			}
		}
	}

	return w, nil
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

	// TODO: executa o insert

	// executa o event handler
	if hasHandler {
		if err := handler.Event(EventParameters{Type: AfterAppend, Context: ctx, Database: db}); err != nil {
			return err
		}
	}

	if !db.WithinTransaction() {
		w.Freeze()
	} else {
		// TODO: armazenar a workarea para dar um freeze depois do commit da transação
	}

	return nil
}

func (w *workarea[T]) Replace(ctx context.Context, db Database) error {
	return nil
}

func (w *workarea[T]) Delete(ctx context.Context, db Database) error {
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
	return As[I](any(w.entity))
}

func As[T any](v any) (T, bool) {
	c, ok := v.(T)
	return c, ok
}
