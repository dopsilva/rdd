package rdd

import (
	"database/sql"

	"github.com/lib/pq"
	sqlite "github.com/mattn/go-sqlite3"
)

type Database interface {
	Exec(q string, args ...any) (sql.Result, error)
	Query(q string, args ...any) (*sql.Rows, error)
	QueryRow(q string, args ...any) *sql.Row

	Close() error

	Engine() DatabaseEngine

	WithinTransaction() bool
	IsDuplicatedError(err error) bool
}

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

func (db *DatabaseWrapper) Close() error {
	return db.db.Close()
}

func (db *DatabaseWrapper) WithinTransaction() bool {
	return false
}

func (db *DatabaseWrapper) IsDuplicatedError(err error) bool {
	if err == nil {
		return false
	}

	if verr, ok := err.(*pq.Error); ok {
		if verr.Code == "23505" {
			return true
		}
	}
	if verr, ok := err.(sqlite.Error); ok {
		if verr.Code == 19 && (verr.ExtendedCode == 1555 || verr.ExtendedCode == 2067) {
			return true
		}
	}
	return false
}

func (db *DatabaseWrapper) Engine() DatabaseEngine {
	return db.engine
}

type TransactionWrapper struct {
	tx *sql.Tx
}
