package rdd

import (
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
	string | int | int64 | bool | float64 | time.Time
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

func (f *Field[T]) Type() reflect.Type {
	return reflect.TypeOf(f.value)
}

func (f *Field[T]) Freeze() {
	f.old = f.value
}
