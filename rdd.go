package rdd

import "database/sql"

func Connect(engine DatabaseEngine, url string) (Database, error) {
	var dbi Database

	switch engine {
	case Cockroach:
		db, err := sql.Open("postgres", url)
		if err != nil {
			return nil, err
		}
		dbi = &DatabaseWrapper{db: db, engine: engine}
	}

	return dbi, nil
}
