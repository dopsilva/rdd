package rdd

import (
	"database/sql"
	"errors"
	"sync"

	"github.com/google/uuid"
	sqlite "github.com/mattn/go-sqlite3"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

var (
	ErrNotFound = errors.New("not found")
)

var onlyOnce sync.Once

// Connect retorna a conexão com o banco de dados através da engine informada e da url de conexão.
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

var (
	registeredSchemas = make(map[string]*TableSchema)
)

// Register registra o schema da entidade.
func Register[T any]() {
	e := Use[T]()
	if v, ok := any(e).(Workarea[T]); ok {
		registeredSchemas[v.Entity()] = v.Schema()
	}
}

func GetRegisteredSchemas() []*TableSchema {
	ret := make([]*TableSchema, len(registeredSchemas))
	i := 0
	for _, v := range registeredSchemas {
		ret[i] = v
		i++
	}
	return ret
}

type Resultset[T any] []*T

func (r Resultset[T]) Close() {
	for _, v := range r {
		any(v).(Workarea[T]).Close()
	}
}

func (r Resultset[L]) Len() int {
	return len(r)
}

func (r Resultset[T]) Empty() bool {
	return len(r) == 0
}

// Select executa a query no banco de dados retornando o resultset da entidade T.
// O ideal nessa função é que seja executada uma query no padrão SQL-92.
func Select[T any](db Database, q string, args ...any) (Resultset[T], error) {
	res := make(Resultset[T], 0)
	empty := false

	// executa a query
	rows, err := db.Query(q, args...)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		} else {
			empty = true
		}
	}

	if !empty {
		// pega os nomes das colunas retornados
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			// cria a workarea
			e := Use[T]()
			w := any(e).(Workarea[T])

			// pega o endereço dos campos do resultset
			fields := w.GetFieldsAddr(columns)

			if len(fields) > 0 {
				// lê as colunas do resultset
				if err := rows.Scan(fields...); err != nil {
					return nil, err
				}
				// armazena a entidade para retorno
				res = append(res, e)
			}
		}
	}

	return res, nil
}
