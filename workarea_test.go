package rdd

import (
	"context"
	"testing"
	"time"
)

func TestRddChanged(t *testing.T) {

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
}

func (u *Usuario) Event(params EventParameters) error {
	return nil
}

func TestWorkAreaEvent(t *testing.T) {
	u, err := Use[Usuario]()
	if err != nil {
		t.Fatal(err)
	}

	u.Append(context.Background(), nil)
}
