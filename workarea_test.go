package rdd

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"testing"
	"time"
)

var (
	testContext  = context.Background()
	testDatabase Database
)

type Usuario struct {
	Workarea[Usuario] `rdd-table:"usuarios"`

	ID          Field[string]         `rdd-column:"id" rdd-primary-key:"true" rdd-auto-generated:"true" rdd-default:"new_uuid"`
	Email       Field[string]         `rdd-column:"email" rdd-unique-key:"true"`
	Nome        Field[string]         `rdd-column:"nome"`
	IncluidoEm  Field[time.Time]      `rdd-column:"incluido_em"`
	IncluidoPor Field[sql.NullString] `rdd-column:"incluido_por" rdd-nullable:"true"`

	ConstraintIncluidoPor Constraint `rdd-foreign-key:"incluido_por" rdd-foreign-key-reference:"usuarios"`
}

func (u *Usuario) BeforeAppend(params EventParameters) error {
	u.IncluidoEm.Set(time.Now())
	return nil
}

func init() {
	Register[Usuario]()
}

func TestMain(m *testing.M) {
	var err error

	var m1, m2 runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&m1)

	testDatabase, err = Connect(SQLite, ":memory:")
	if err != nil {
		panic(err)
	}
	defer testDatabase.Close()

	// criar as tabelas de teste
	for _, v := range registeredSchemas {
		if err := tableCreate(testDatabase, v, &CreateTableOptions{IfNotExists: true}); err != nil {
			panic(err)
		}
	}

	m.Run()

	runtime.ReadMemStats(&m2)

	fmt.Println("alloc  :", m2.TotalAlloc-m1.TotalAlloc)
	fmt.Println("mallocs:", m2.Mallocs-m1.Mallocs)
}

func TestUpdate(t *testing.T) {
	defer truncateTable(testDatabase, "usuarios")

	u := Use[Usuario]()
	defer Close[Usuario](u)

	u.Email.Set("dopslv@gmail.com")
	u.Nome.Set("Daniel")
	u.IncluidoEm.Set(time.Now())

	if err := u.Append(testContext, testDatabase); err != nil {
		t.Fatal(err)
	}

	u.Nome.Set("Daniel Ortiz")

	if err := u.Replace(testContext, testDatabase); err != nil {
		t.Fatal(err)
	}

	var nome string
	if err := testDatabase.QueryRow("select nome from usuarios where id = $1", u.ID.Get()).Scan(&nome); err != nil {
		t.Fatal(err)
	}

	res, err := Select[Usuario](testDatabase, "select nome from usuarios where id = $1", u.ID.Get())
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	if len(res) == 0 {
		t.Fatal("esperado retorno do usuário")
	}

	if res[0].Nome.Get() != u.Nome.Get() {
		t.Fatalf("esperado %s obtido %s", u.Nome.Get(), nome)
	}

}

func TestChanged(t *testing.T) {
	u := Use[Usuario]()
	defer Close(u)

	type data struct {
		Email      string    `rdd-column:"email"`
		Nome       string    `rdd-column:"nome"`
		IncluidoEm time.Time `rdd-column:"incluido_em"`
	}

	var tests = []struct {
		data    data
		changed bool
	}{
		{changed: false},
		{data: data{IncluidoEm: time.Now()}, changed: true},
		{data: data{Email: "dopslv@gmail.com", Nome: "Daniel Ortiz Pereira da Silva"}, changed: true},
	}

	for _, test := range tests {
		u.Load(test.data)

		changed := u.Changed()
		if changed != test.changed {
			t.Fatalf("expected %t got %t", test.changed, changed)
		}
	}
}

func TestWorkAreaEvent(t *testing.T) {
	defer truncateTable(testDatabase, "usuarios")

	u := Use[Usuario]()

	u.Email.Set("dopslv@gmail.com")
	u.Nome.Set("Daniel")
	u.IncluidoEm.Set(time.Now())

	if err := u.Append(testContext, testDatabase); err != nil {
		t.Fatal(err)
	}
}

func truncateTable(db Database, table string) {
	// TODO: tratar outros bancos de dados
	_, err := db.Exec("delete from " + table)
	if err != nil {
		panic(err)
	}
}

func TestTransaction(t *testing.T) {
	defer truncateTable(testDatabase, "usuarios")

	u := Use[Usuario]()

	u.Email.Set("dopslv@gmail.com")
	u.Nome.Set("Daniel")
	u.IncluidoEm.Set(time.Now())

	if err := u.Append(testContext, testDatabase); err != nil {
		t.Fatal(err)
	}

	// inicia a transação
	tx1, err := testDatabase.Begin()
	if err != nil {
		t.Fatal(err)
	}

	u.Nome.Set("transação 1")

	if err := u.Replace(testContext, tx1); err != nil {
		t.Fatal(err)
	}

	// inicia o savepoint
	tx2, err := tx1.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// NOTE: neste caso aqui não é certo alterar a mesma workarea
	// dentro de um savepoint pois o rollback é no banco de dados
	// e não dá pra voltar o valor antes do savepoint
	u.Nome.Set("transação 2")

	if err := u.Replace(testContext, tx2); err != nil {
		t.Fatal(err)
	}

	// rollback no savepoint
	if err := tx2.Rollback(); err != nil {
		t.Fatal(err)
	}

	// commit da transação
	if err := tx1.Commit(testContext); err != nil {
		t.Fatal(err)
	}

	res, err := Select[Usuario](testDatabase, "select nome from usuarios where id = $1", u.ID.Get())
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	if res.Empty() {
		t.Fatal("esperado retorno do usuário")
	}
	if res[0].Nome.Get() != "transação 1" {
		t.Fatalf("esperado %s obtido %s", "transação 1", res[0].Nome.Get())
	}
}

func TestSelect(t *testing.T) {
	defer truncateTable(testDatabase, "usuarios")

	var usuarios = []struct {
		Email string `rdd-column:"email"`
		Nome  string `rdd-column:"nome"`
	}{
		{Email: "usuario1@gmail.com", Nome: "Usuario 1"},
		{Email: "usuario2@gmail.com", Nome: "Usuario 2"},
		{Email: "usuario3@gmail.com", Nome: "Usuario 3"},
	}

	u := Use[Usuario]()
	defer Close(u)

	for _, v := range usuarios {

		if err := u.Load(v); err != nil {
			t.Fatal(err)
		}

		if err := u.Append(testContext, testDatabase); err != nil {
			t.Fatal(err)
		}

		u.Reset()
	}

	res, err := Select[Usuario](testDatabase, "select nome from usuarios")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	if res.Len() != len(usuarios) {
		t.Fatalf("esperado %d obtido %d", len(usuarios), len(res))
	}

}
