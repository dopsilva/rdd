package rdd

import (
	"database/sql"
	"sync"

	"github.com/google/uuid"
	sqlite "github.com/mattn/go-sqlite3"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

var onlyOnce sync.Once

func Connect(engine DatabaseEngine, url string) (Database, error) {
	var dbw Database
	var db *sql.DB
	var err error

	switch engine {
	case Cockroach:
		db, err = sql.Open("postgres", url)
		if err != nil {
			return nil, err
		}

	case SQLite:
		onlyOnce.Do(func() {
			sql.Register("sqlite3_rdd", &sqlite.SQLiteDriver{
				ConnectHook: func(conn *sqlite.SQLiteConn) error {
					if err := conn.RegisterFunc("gen_random_uuid", func() string {
						return uuid.NewString()
					}, true); err != nil {
						return err
					}
					return nil
				},
			})
		})

		db, err = sql.Open("sqlite3_rdd", url)
		if err != nil {
			return nil, err
		}
	}

	dbw = &DatabaseWrapper{db: db, engine: engine}

	return dbw, nil
}
