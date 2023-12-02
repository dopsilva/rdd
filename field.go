package rdd

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"time"
)

type Typed interface {
	Type() reflect.Type
}

type Changeable interface {
	Changed() bool
}

type Freezable interface {
	Freeze()
}

type Resetable interface {
	Reset()
}

type Fieldable[T comparable] interface {
	string | int64 | bool | float64 | time.Time | sql.NullString | sql.NullInt64 | sql.NullBool | sql.NullFloat64 | sql.NullTime
}

type Field[T Fieldable[T]] struct {
	value T
	old   T
}

// Get obtém o valor do campo
func (f *Field[T]) Get() T {
	return f.value
}

// Set define o valor do campo
func (f *Field[T]) Set(value T) {
	f.value = value
}

// Changed verifica se houve alteração no campo
func (f *Field[T]) Changed() bool {
	switch f.Type().Kind() {
	case reflect.String, reflect.Int, reflect.Int64, reflect.Float64, reflect.Bool:
		return f.value != f.old
	default:
		switch any(f.value).(type) {
		case time.Time:
			if (any(f.value).(time.Time)).Compare(any(f.old).(time.Time)) != 0 {
				return true
			}
		case sql.NullString:
			v := any(f.value).(sql.NullString)
			o := any(f.old).(sql.NullString)
			if v.Valid != o.Valid || v.String != o.String {
				return true
			}
		case sql.NullInt64:
			v := any(f.value).(sql.NullInt64)
			o := any(f.old).(sql.NullInt64)
			if v.Valid != o.Valid || v.Int64 != o.Int64 {
				return true
			}
		case sql.NullBool:
			v := any(f.value).(sql.NullBool)
			o := any(f.old).(sql.NullBool)
			if v.Valid != o.Valid || v.Bool != o.Bool {
				return true
			}
		case sql.NullFloat64:
			v := any(f.value).(sql.NullFloat64)
			o := any(f.old).(sql.NullFloat64)
			if v.Valid != o.Valid || v.Float64 != o.Float64 {
				return true
			}
		case sql.NullTime:
			v := any(f.value).(sql.NullTime)
			o := any(f.old).(sql.NullTime)
			if v.Valid != o.Valid || v.Time.Compare(o.Time) != 0 {
				return true
			}
		default:
			panic(fmt.Sprintf("unsupported type %T", f.value))
		}
	}
	return false
}

// Zero retorna o valor zero do tipo de dado do campo
func (f *Field[T]) Zero() T {
	var i T
	return i
}

// Reset zera os valores
func (f *Field[T]) Reset() {
	var zero T = f.Zero()
	f.value = zero
	f.old = zero
}

// Empty retorna se o conteudo está vazio
func (f *Field[T]) Empty() bool {
	switch v := any(f.value).(type) {
	case nil:
		return true
	case string:
		return v == ""
	case *string:
		return v == nil || *v == ""
	case int64:
		return v == 0
	case float64:
		return v == 0
	case time.Time:
		return v.IsZero()
	}
	return any(f.value) == any(f.Zero())
}

// Type retorna o tipo nativo do dado
func (f *Field[T]) Type() reflect.Type {
	return reflect.TypeOf(f.value)
}

// Freeze resfria o campo tornando iguais o antigo e o atual valor
func (f *Field[T]) Freeze() {
	f.old = f.value
}

// Value implementa a interface sql.Valuer
func (f *Field[T]) Value() (driver.Value, error) {
	return f.value, nil
}

// Value implementa a interface sql.Scanner
func (f *Field[T]) Scan(value any) error {
	f.Set(value.(T))
	return nil
}

type Constraint struct{}
