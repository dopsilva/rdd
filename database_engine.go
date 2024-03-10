package rdd

import "fmt"

type DatabaseEngine int

const (
	SQLite DatabaseEngine = iota + 1
	Cockroach
	SQLServer
)

func (e DatabaseEngine) QuotedIdentifier(v any) string {
	switch e {
	case SQLite, Cockroach:
		return fmt.Sprintf("\"%s\"", v)
	}
	panic("engine não esperada")
}

func (e DatabaseEngine) DefaultRandomUUID() string {
	switch e {
	case SQLite:
		return "(gen_random_uuid())"
	case Cockroach:
		return "gen_random_uuid()"
	}
	panic("engine não esperada")
}

func (e DatabaseEngine) DefaultCurrentTimestamp() string {
	switch e {
	case SQLite:
		return "current_timestamp"
	case Cockroach:
		return "current_timestamp()"
	}
	panic("engine não esperada")
}
