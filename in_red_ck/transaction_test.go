package in_red_ck

import (
	"cmp"
	"fmt"
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
	SSN    string
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
			SSN:    "1234",
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

const nodeSlotLength = 50

func Test_SimpleAddPerson(t *testing.T) {
	kafka.Initialize(kafka.DefaultConfig)
	t.Logf("Transaction story, single b-tree, person record test.\n")
	trans, err := NewTransaction(true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()

	pk, p := newPerson("joe", "krueger", "male", "email", "phone")

	b3, err := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", trans)
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

func Test_TwoTransactionsWithNoConflict(t *testing.T) {
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

func Test_AddAndSearchManyPersons(t *testing.T) {
	trans, err := NewTransaction(true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}

	trans.Begin()
	b3, err := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", trans)
	if err != nil {
		trans.Rollback(ctx)
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}

	const start = 1
	end := start + 50

	for i := start; i < end; i++ {
		pk, p := newPerson(fmt.Sprintf("tracy%d", i), "swift", "female", "email", "phone")
		if ok, err := b3.Add(ctx, pk, p); !ok || err != nil {
			t.Errorf("b3.Add('tracy') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
			trans.Rollback(ctx)
			return
		}
	}
	if err := trans.Commit(ctx); err != nil {
		t.Errorf(err.Error())
		t.Fail()
		return
	}

	trans, err = NewTransaction(false, -1)
	if err != nil {
		t.Errorf(err.Error())
		t.Fail()
		return
	}

	if err := trans.Begin(); err != nil {
		t.Errorf(err.Error())
		t.Fail()
		return
	}

	b3, err = OpenBtree[PersonKey, Person](ctx, "persondb", trans)
	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}
	for i := start; i < end; i++ {
		pk, _ := newPerson(fmt.Sprintf("tracy%d", i), "swift", "female", "email", "phone")
		if ok, err := b3.FindOne(ctx, pk, true); !ok || err != nil {
			t.Errorf("b3.FIndOne('%s') failed, got(ok, err) = %v, %v, want = true, nil.", pk.Firstname, ok, err)
			return
		}
	}

	trans.Commit(ctx)
}

// This test took about 3 minutes from empty to finish in my laptop.
func Test_VolumeAddThenSearch(t *testing.T) {
	start := 9001
	end := 100000
	batchSize := 100

	t1, _ := NewTransaction(true, -1)
	t1.Begin()
	b3, _ := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)

	// Populating 90,000 items took about few minutes. Not bad considering I did not use Kafka queue
	// for scheduled batch deletes.
	for i := start; i <= end; i++ {
		pk, p := newPerson("jack", fmt.Sprintf("reepper%d", i), "male", "email very very long long long", "phone123")
		if ok, _ := b3.AddIfNotExist(ctx, pk, p); ok {
			t.Logf("%v inserted", pk)
		}
		if i % batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.Fail()
			}
			t1, _ = NewTransaction(true, -1)
			t1.Begin()
			b3, _ = NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
		}
	}

	// Search them all. Searching 90,000 items just took few seconds in my laptop.
	for i := start; i <= end; i++ {
		lname := fmt.Sprintf("reepper%d", i)
		pk, _ := newPerson("jack", lname, "male", "email very very long long long", "phone123")
		if found, err := b3.FindOne(ctx, pk, false); !found || err != nil {
			t.Error(err)
			t.Fail()
		}
		ci, _ := b3.GetCurrentItem(ctx)
		if ci.Value.Phone != "phone123" || ci.Key.Lastname != lname {
			t.Error(fmt.Errorf("Did not find the correct person with phone123 & lname %s", lname))
			t.Fail()
		}
		if i % batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.Fail()
			}
			t1, _ = NewTransaction(false, -1)
			t1.Begin()
			b3, _ = NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
		}
	}
}

// Add prefix Test_ if wanting to run this test.
func VolumeDeletes(t *testing.T) {
	start := 9001
	end := 100000
	batchSize := 100

	t1, _ := NewTransaction(true, -1)
	t1.Begin()
	b3, _ := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)

	// Populating 90,000 items took about few minutes, did not use Kafka based delete service.
	for i := start; i <= end; i++ {
		pk, _ := newPerson("jack", fmt.Sprintf("reepper%d", i), "male", "email very very long long long", "phone123")
		if ok, err := b3.Remove(ctx, pk); !ok || err != nil {
			if err != nil {
				t.Error(err)
			}
			// Ignore not found as item could had been deleted in previous run.
		}
		if i % batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.Fail()
			}
			t1, _ = NewTransaction(true, -1)
			t1.Begin()
			b3, _ = NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
		}
	}
}
