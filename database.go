package rdd

import (
	"context"
	"database/sql"
	"strings"

	"github.com/google/uuid"
	"github.com/lib/pq"
	sqlite "github.com/mattn/go-sqlite3"
)

type Database interface {
	Exec(q string, args ...any) (sql.Result, error)
	Query(q string, args ...any) (*sql.Rows, error)
	QueryRow(q string, args ...any) *sql.Row

	Begin() (Database, error)
	Commit(ctx context.Context) error
	Rollback() error

	Close() error

	Engine() DatabaseEngine

	WithinTransaction() bool
	IsDuplicatedError(err error) bool

	StoreWorkarea(Freezable)
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

func (db *DatabaseWrapper) Begin() (Database, error) {
	tx, err := db.db.Begin()
	return &TransactionWrapper{db: db, tx: tx}, err
}

func (db *DatabaseWrapper) Commit(ctx context.Context) error {
	return nil
}

func (db *DatabaseWrapper) Rollback() error {
	return nil
}

func (db *DatabaseWrapper) Close() error {
	return db.db.Close()
}

func (db *DatabaseWrapper) WithinTransaction() bool {
	return false
}

func (db *DatabaseWrapper) IsDuplicatedError(err error) bool {
	return IsDuplicatedError(err)
}

func (db *DatabaseWrapper) StoreWorkarea(f Freezable) {
}

func (db *DatabaseWrapper) Engine() DatabaseEngine {
	return db.engine
}

type TransactionWrapper struct {
	db        *DatabaseWrapper
	tx        *sql.Tx
	workareas []Freezable
	savepoint bool
	spname    string // savepoint name
}

func (tx *TransactionWrapper) Exec(q string, args ...any) (sql.Result, error) {
	return tx.tx.Exec(q, args...)
}

func (tx *TransactionWrapper) Query(q string, args ...any) (*sql.Rows, error) {
	return tx.tx.Query(q, args...)
}

func (tx *TransactionWrapper) QueryRow(q string, args ...any) *sql.Row {
	return tx.tx.QueryRow(q, args...)
}

func (tx *TransactionWrapper) Close() error {
	return nil
}

func (tx *TransactionWrapper) Begin() (Database, error) {

	ntx := &TransactionWrapper{tx: tx.tx, db: tx.db}
	ntx.savepoint = true
	ntx.spname = "sp_" + strings.ReplaceAll(uuid.NewString(), "-", "")

	if _, err := tx.Exec("savepoint " + ntx.spname); err != nil {
		return nil, err
	}

	return ntx, nil
}

func (tx *TransactionWrapper) Commit(ctx context.Context) error {
	if !tx.savepoint {
		if err := tx.tx.Commit(); err != nil {
			return err
		}
		// congela as workareas
		for _, w := range tx.workareas {
			// dispara o evento AfterCommit da workarea
			any(w).(Customizable).AfterCommit(EventParameters{Context: ctx, Database: tx.db})
			w.Freeze()
		}
	} else {
		if _, err := tx.Exec("release savepoint " + tx.spname); err != nil {
			return err
		}
	}

	return nil
}

func (tx *TransactionWrapper) Rollback() error {
	if !tx.savepoint {
		if err := tx.tx.Rollback(); err != nil {
			return err
		}
		// congela as workareas
		for _, w := range tx.workareas {
			w.Freeze()
		}
	} else {
		if _, err := tx.Exec("rollback to " + tx.spname); err != nil {
			return err
		}
	}
	return nil
}

func (tx *TransactionWrapper) WithinTransaction() bool {
	return true
}

func (tx *TransactionWrapper) IsDuplicatedError(err error) bool {
	return IsDuplicatedError(err)
}

func (tx *TransactionWrapper) Engine() DatabaseEngine {
	return tx.db.Engine()
}

func (tx *TransactionWrapper) StoreWorkarea(f Freezable) {
	tx.workareas = append(tx.workareas, f)
}

func IsDuplicatedError(err error) bool {
	if err == nil {
		return false
	}

	// cockroachdb/postgres
	if verr, ok := err.(*pq.Error); ok {
		if verr.Code == "23505" {
			return true
		}
	}

	// sqlite
	if verr, ok := err.(sqlite.Error); ok {
		if verr.Code == 19 && (verr.ExtendedCode == 1555 || verr.ExtendedCode == 2067) {
			return true
		}
	}

	return false
}
