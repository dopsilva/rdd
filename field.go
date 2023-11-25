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

type Fieldable[T comparable] interface {
	string | int | int64 | bool | float64 | time.Time | sql.NullString | sql.NullInt64 | sql.NullBool | sql.NullFloat64 | sql.NullTime
}

type Field[T Fieldable[T]] struct {
	value T
	old   T
}

func (f *Field[T]) Get() T {
	return f.value
}

func (f *Field[T]) Set(value T) {
	f.value = value
}

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
		default:
			panic(fmt.Sprintf("unsupported type %T", f.value))
		}
	}
	return false
}

func (f *Field[T]) Zero() T {
	var i T
	return i
}

func (f *Field[T]) Reset() {
	f.value = f.Zero()
	f.old = f.Zero()
}

func (f *Field[T]) Empty() bool {
	switch v := any(f.value).(type) {
	case nil:
		return true
	case string:
		return v == ""
	case *string:
		return v == nil || *v == ""
	case int:
		return v == 0
	case int8:
		return v == 0
	case int16:
		return v == 0
	case int32:
		return v == 0
	case int64:
		return v == 0
	case uint:
		return v == 0
	case uint8:
		return v == 0
	case uint16:
		return v == 0
	case uint32:
		return v == 0
	case uint64:
		return v == 0
	case float32:
		return v == 0
	case float64:
		return v == 0
	case *int:
		return v == nil || *v == 0
	case *int8:
		return v == nil || *v == 0
	case *int16:
		return v == nil || *v == 0
	case *int32:
		return v == nil || *v == 0
	case *int64:
		return v == nil || *v == 0
	case *uint:
		return v == nil || *v == 0
	case *uint8:
		return v == nil || *v == 0
	case *uint16:
		return v == nil || *v == 0
	case *uint32:
		return v == nil || *v == 0
	case *uint64:
		return v == nil || *v == 0
	case *float32:
		return v == nil || *v == 0
	case *float64:
		return v == nil || *v == 0
	case *any:
		return *v == nil
	case *bool:
		return v == nil
	case time.Time:
		return v.IsZero()
	case *time.Time:
		return v == nil || (*v).IsZero()
	}
	return any(f.value) == any(f.Zero())
}

func (f *Field[T]) Type() reflect.Type {
	return reflect.TypeOf(f.value)
}

func (f *Field[T]) Freeze() {
	f.old = f.value
}

func (f *Field[T]) Value() (driver.Value, error) {
	return f.value, nil
}

func (f *Field[T]) Scan(value any) error {
	f.Set(value.(T))
	return nil
}
