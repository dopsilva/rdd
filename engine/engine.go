package engine

type Engine int

const (
	SQLite Engine = iota + 1
	Cockroach
	SQLServer
)
