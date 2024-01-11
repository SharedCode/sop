package in_red_ck

import (
	"cmp"
	"testing"

	"github.com/SharedCode/sop/in_red_ck/kafka"
)

type PersonKey struct {
	Firstname string
	Lastname  string
}

type Person struct {
	Gender string
	Email  string
	Phone  string
}

func newPerson(fname string, lname string, gender string, email string, phone string) (PersonKey, Person) {
	return PersonKey{
			Firstname: fname,
			Lastname:  lname,
		},
		Person{
			Gender: gender,
			Email:  email,
			Phone:  phone,
		}
}
func (x PersonKey) Compare(other interface{}) int {
	y := other.(PersonKey)
	i := cmp.Compare[string](x.Lastname, y.Lastname)
	if i != 0 {
		return i
	}
	return cmp.Compare[string](x.Firstname, y.Firstname)
}

func Test_SimpleAddPerson(t *testing.T) {
	kafka.Initialize(kafka.DefaultConfig)
	t.Logf("Transaction story, single b-tree, person record test.\n")
	trans, err := NewTransaction(true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()

	pk, p := newPerson("joe", "krueger", "male", "email", "phone")

	b3, err := NewBtree[PersonKey, Person](ctx, "persondb", 4, false, false, false, "", trans)
	if err != nil {
		trans.Rollback(ctx)
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}
	if ok, err := b3.Add(ctx, pk, p); !ok || err != nil {
		t.Errorf("Add('joe') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}

	if ok, err := b3.FindOne(ctx, pk, false); !ok || err != nil {
		t.Errorf("FindOne('joe',false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}
	if k, err := b3.GetCurrentKey(ctx); k.Firstname != pk.Firstname || err != nil {
		t.Errorf("GetCurrentKey() failed, got = %v, %v, want = 1, nil.", k, err)
		trans.Rollback(ctx)
		return
	}
	if v, err := b3.GetCurrentValue(ctx); v.Phone != p.Phone || err != nil {
		t.Errorf("GetCurrentValue() failed, got = %v, %v, want = 1, nil.", v, err)
		trans.Rollback(ctx)
		return
	}
	t.Logf("Successfully added & found item with key 'joe'.")
	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}
}

func Test_TwoTransactionsWithConflict(t *testing.T) {
	t.Logf("Transaction story, single b-tree, person record test.\n")

	trans, err := NewTransaction(true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}

	trans2, err := NewTransaction(true, -1)

	trans.Begin()
	trans2.Begin()

	pk, p := newPerson("tracy", "swift", "female", "email", "phone")
	b3, err := OpenBtree[PersonKey, Person](ctx, "persondb", trans)
	if err != nil {
		trans.Rollback(ctx)
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}
	if ok, err := b3.Add(ctx, pk, p); !ok || err != nil {
		t.Errorf("b3.Add('tracy') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}

	b32, err := OpenBtree[PersonKey, Person](ctx, "persondb", trans2)
	if err != nil {
		trans2.Rollback(ctx)
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}
	if ok, err := b32.Add(ctx, pk, p); !ok || err != nil {
		t.Errorf("b32.Add('tracy') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}

	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit 1 returned error, details: %v.", err)
	}
	if err := trans2.Commit(ctx); err != nil {
		t.Errorf("Commit 2 returned error, details: %v.", err)
	}
}
