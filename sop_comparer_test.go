package sop

import (
	"cmp"
	"testing"

	sop "sop/in_memory"
)

type personKey struct {
	firstname string
	lastname  string
}

type person struct {
	personKey
	gender string
	email  string
	phone  string
}

func newPerson(fname string, lname string, gender string, email string, phone string) person {
	return person{
		personKey: personKey{
			firstname: fname,
			lastname:  lname,
		},
		gender: gender,
		email:  email,
		phone:  phone,
	}
}
func (x personKey) Compare(other interface{}) int {
	y := other.(personKey)
	i := cmp.Compare[string](x.lastname, y.lastname)
	if i != 0 {
		return i
	}
	return cmp.Compare[string](x.firstname, y.firstname)
}

func Test_PersonLookup(t *testing.T) {
	t.Log("Btree demo, used as a person struct lookup.\n")

	p := newPerson("joe", "krueger", "male", "email", "phone")
	b3 := sop.NewBtree[personKey, person](false)
	b3.Add(p.personKey, p)

	if b3.FindOne(p.personKey, false) {
		t.Logf("Person w/ key %v found.", b3.GetCurrentKey())
	}

	if !passBtreeAround(b3) {
		t.Logf("passBtreeAround(b3) failed, got false, want true.")
	}

	t.Log("Btree demo, used as a person struct lookup end.\n")
}

func passBtreeAround(b3 sop.BtreeInterface[personKey, person]) bool {
	key := personKey{firstname: "joe", lastname: "krueger"}
	return b3.FindOne(key, false)
}
