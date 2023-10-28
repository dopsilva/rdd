package rdd

import "database/sql"

type Database interface {
	Exec(q string, args ...any) (sql.Result, error)
	Query(q string, args ...any) (*sql.Rows, error)
	QueryRow(q string, args ...any) *sql.Row

	WithinTransaction() bool
}

type DatabaseEngine int

const (
	Cockroach DatabaseEngine = iota
	SQLite
)

type DatabaseWrapper struct {
	db     *sql.DB
	engine DatabaseEngine
}

func (db *DatabaseWrapper) Exec(q string, args ...any) (sql.Result, error) {
	return db.db.Exec(q, args...)
}

func (db *DatabaseWrapper) Query(q string, args ...any) (*sql.Rows, error) {
	return db.db.Query(q, args...)
}

func (db *DatabaseWrapper) QueryRow(q string, args ...any) *sql.Row {
	return db.db.QueryRow(q, args...)
}

func (db *DatabaseWrapper) WithinTransaction() bool {
	return false
}

type TransactionWrapper struct {
	tx *sql.Tx
}
