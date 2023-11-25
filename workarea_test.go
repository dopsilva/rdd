package rdd

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

var (
	testContext = context.Background()
)

func TestCreateTable(t *testing.T) {
	type Usuario struct {
		Workarea[Usuario] `rdd-table:"usuarios"`

		ID          Field[string]         `rdd-column:"id" rdd-nullable:"false" rdd-primary-key:"true"`
		Email       Field[string]         `rdd-column:"email" rdd-nullable:"false" rdd-unique-key:"true"`
		Nome        Field[string]         `rdd-column:"nome" rdd-nullable:"false"`
		IncluidoEm  Field[time.Time]      `rdd-column:"incluido_em"`
		IncluidoPor Field[sql.NullString] `rdd-column:"incluido_por"`
	}

	db, err := Connect(SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	u, err := Use[Usuario]()
	if err != nil {
		t.Fatal(err)
	}

	if err := u.CreateTable(testContext, db, &CreateTableOptions{IfNotExists: true}); err != nil {
		t.Fatal(err)
	}
}

func TestUpdate(t *testing.T) {
	type Usuario struct {
		Workarea[Usuario] `rdd-table:"usuarios"`

		ID    Field[string] `rdd-column:"id" rdd-nullable:"false" rdd-primary-key:"true"`
		Email Field[string] `rdd-column:"email" rdd-nullable:"false" rdd-unique-key:"true"`
		Nome  Field[string] `rdd-column:"nome" rdd-nullable:"false"`
	}

	db, err := Connect(SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	u, err := Use[Usuario]()
	if err != nil {
		t.Fatal(err)
	}

	if err := u.CreateTable(testContext, db, &CreateTableOptions{IfNotExists: true}); err != nil {
		t.Fatal(err)
	}

	u.Email.Set("dopslv@gmail.com")
	u.Nome.Set("Daniel")

	if err := u.Append(testContext, db); err != nil {
		t.Fatal(err)
	}

	u.Nome.Set("Daniel Ortiz")

	if err := u.Replace(testContext, db); err != nil {
		t.Fatal(err)
	}

	var nome string
	if err := db.QueryRow("select nome from usuarios where id = $1", u.ID.Get()).Scan(&nome); err != nil {
		t.Fatal(err)
	}

	if nome != u.Nome.Get() {
		t.Fatalf("esperado %s obtido %s", u.Nome.Get(), nome)
	}

}

func TestChanged(t *testing.T) {

	type Usuario struct {
		Workarea[Usuario] `rdd-table:"usuarios"`

		ID         Field[string]    `rdd-column:"id"`
		Email      Field[string]    `rdd-column:"email"`
		Nome       Field[string]    `rdd-column:"nome"`
		IncluidoEm Field[time.Time] `rdd-column:"incluido_em"`
	}

	u, err := Use[Usuario]()
	if err != nil {
		t.Fatal(err)
	}

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

type Usuario struct {
	Workarea[Usuario] `rdd-table:"usuarios"`

	ID    Field[string] `rdd-column:"id" rdd-nullable:"false" rdd-primary-key:"true" rdd-auto-generated:"true" rdd-default:"new_uuid"`
	Email Field[string] `rdd-column:"email" rdd-nullable:"false" rdd-unique-key:"true"`
}

func (u *Usuario) Event(params EventParameters) error {
	//fmt.Printf("%#v\n", params)
	return nil
}

func TestWorkAreaEvent(t *testing.T) {
	db, err := Connect(SQLite, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	u, err := Use[Usuario]()
	if err != nil {
		t.Fatal(err)
	}

	if err := u.CreateTable(testContext, db, &CreateTableOptions{IfNotExists: true}); err != nil {
		t.Fatal(err)
	}

	u.Email.Set("dopslv@gmail.com")

	if err := u.Append(testContext, db); err != nil {
		t.Fatal(err)
	}

}
