package common

import (
	"cmp"
	"fmt"
	"testing"

	"github.com/sharedcode/sop"
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
	return PersonKey{fname, lname}, Person{gender, email, phone, "1234"}
}

func Compare(x PersonKey, y PersonKey) int {
	i := cmp.Compare[string](x.Lastname, y.Lastname)
	if i != 0 {
		return i
	}
	return cmp.Compare[string](x.Firstname, y.Firstname)
}

const nodeSlotLength = 500
const batchSize = 200

func Test_Rollback(t *testing.T) {
	trans, _ := newMockTransaction(t, sop.ForWriting, -1)
	trans.Begin()

	b3, _ := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, trans, Compare)

	pk, p := newPerson("joe", "shroeger", "male", "email", "phone")
	b3.Add(ctx, pk, p)

	trans.Commit(ctx)

	trans, _ = newMockTransaction(t, sop.ForWriting, -1)
	trans.Begin()

	pk, p = newPerson("joe", "shroeger", "male", "email2", "phone2")
	b3.Update(ctx, pk, p)

	trans.Rollback(ctx)

	trans, _ = newMockTransaction(t, sop.ForReading, -1)
	trans.Begin()
	b3, _ = NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, trans, Compare)

	pk, _ = newPerson("joe", "shroeger", "male", "email", "phone")

	b3.Find(ctx, pk, false)
	v, _ := b3.GetCurrentValue(ctx)

	if v.Email != "email" {
		t.Errorf("Rollback did not restore person record, email got = %s, want = 'email'.", v.Email)
	}
	trans.Commit(ctx)
}

func Test_SimpleAddPerson(t *testing.T) {
	trans, err := newMockTransaction(t, sop.ForWriting, -1)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()

	pk, p := newPerson("joe", "krueger", "male", "email", "phone")

	b3, err := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, trans, Compare)

	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}
	if ok, err := b3.Add(ctx, pk, p); !ok || err != nil {
		t.Errorf("Add('joe') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}

	if ok, err := b3.Find(ctx, pk, false); !ok || err != nil {
		t.Errorf("FindOne('joe',false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}
	if k := b3.GetCurrentKey().Key; k.Firstname != pk.Firstname {
		// Rollback before generating an error.
		trans.Rollback(ctx)
		t.Errorf("GetCurrentKey() failed, got = %v, %v, want = 1, nil.", k, err)
		return
	}
	if v, err := b3.GetCurrentValue(ctx); v.Phone != p.Phone || err != nil {
		t.Errorf("GetCurrentValue() failed, got = %v, %v, want = 1, nil.", v, err)
		return
	}
	t.Logf("Successfully added & found item with key 'joe'.")
	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}
}

func Test_NoCheckCommitAddFail(t *testing.T) {
	trans, err := newMockTransaction(t, sop.NoCheck, -1)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()

	pk, p := newPerson("joe", "krueger", "male", "email", "phone")

	b3, _ := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondbnc",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, trans, Compare)

	if _, err := b3.Add(ctx, pk, p); err == nil {
		t.Errorf("Add('joe') failed, got(ok, err) = nil, want = err, nil.")
	}
}

func Test_NoCheckCommit(t *testing.T) {
	trans, err := newMockTransaction(t, sop.ForWriting, -1)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()

	pk, p := newPerson("joe", "krueger", "male", "email", "phone")

	b3, _ := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondbnc",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, trans, Compare)

	if _, err := b3.Add(ctx, pk, p); err != nil {
		t.Errorf("Add('joe') failed, got(ok, err) = %v, want = nil.", err)
	}
	trans.Commit(ctx)

	trans, err = newMockTransaction(t, sop.NoCheck, -1)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()

	b3, _ = OpenBtree[PersonKey, Person](ctx, "persondbnc", trans, Compare)
	b3.Find(ctx, pk, false)

	trans.Commit(ctx)
}

func Test_TwoTransactionsWithNoConflict(t *testing.T) {
	trans, err := newMockTransaction(t, sop.ForWriting, -1)
	if err != nil {
		t.Fatal(err.Error())
	}

	trans2, _ := newMockTransaction(t, sop.ForWriting, -1)

	trans.Begin()
	trans2.Begin()

	pk, p := newPerson("tracy", "swift", "female", "email", "phone")
	b3, err := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, trans, Compare)

	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}
	if ok, err := b3.Add(ctx, pk, p); !ok || err != nil {
		t.Errorf("b3.Add('tracy') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}

	b32, err := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, trans, Compare)

	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}
	if ok, err := b32.Add(ctx, pk, p); !ok || err != nil {
		t.Errorf("b32.Add('tracy') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
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
	trans, err := newMockTransaction(t, sop.ForWriting, -1)
	if err != nil {
		t.Fatal(err.Error())
	}

	trans.Begin()
	b3, err := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, trans, Compare)

	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}

	const start = 1
	end := start + batchSize

	for i := start; i < end; i++ {
		pk, p := newPerson(fmt.Sprintf("tracy%d", i), "swift", "female", "email", "phone")
		if ok, err := b3.Add(ctx, pk, p); !ok || err != nil {
			t.Errorf("b3.Add('tracy') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
			return
		}
	}
	if err := trans.Commit(ctx); err != nil {
		t.Error(err.Error())
		t.Fail()
		return
	}

	trans, err = newMockTransaction(t, sop.ForReading, -1)
	if err != nil {
		t.Error(err.Error())
		t.Fail()
		return
	}

	if err := trans.Begin(); err != nil {
		t.Error(err.Error())
		t.Fail()
		return
	}

	b3, err = OpenBtree[PersonKey, Person](ctx, "persondb", trans, Compare)
	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}
	for i := start; i < end; i++ {
		pk, _ := newPerson(fmt.Sprintf("tracy%d", i), "swift", "female", "email", "phone")
		if ok, err := b3.Find(ctx, pk, true); !ok || err != nil {
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

	t1, _ := newMockTransaction(t, sop.ForWriting, -1)
	t1.Begin()
	b3, _ := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, t1, Compare)

	// Populating 90,000 items took about few minutes. Not bad considering I did not use Kafka queue
	// for scheduled batch deletes.
	for i := start; i <= end; i++ {
		pk, p := newPerson("jack", fmt.Sprintf("reepper%d", i), "male", "email very very long long long", "phone123")
		if ok, _ := b3.AddIfNotExist(ctx, pk, p); ok {
			t.Logf("%v inserted", pk)
		}
		if i%batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.Fail()
			}
			t1, _ = newMockTransaction(t, sop.ForWriting, -1)
			t1.Begin()
			b3, _ = NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
				Name:                     "persondb",
				SlotLength:               nodeSlotLength,
				IsUnique:                 false,
				IsValueDataInNodeSegment: false,
				LeafLoadBalancing:        false,
				Description:              "",
			}, t1, Compare)
		}
	}

	// Search them all. Searching 90,000 items just took few seconds in my laptop.
	for i := start; i <= end; i++ {
		lname := fmt.Sprintf("reepper%d", i)
		pk, _ := newPerson("jack", lname, "male", "email very very long long long", "phone123")
		if found, err := b3.Find(ctx, pk, false); !found || err != nil {
			t.Error(err)
			t.Fail()
		}
		ci, _ := b3.GetCurrentItem(ctx)
		if ci.Value.Phone != "phone123" || ci.Key.Lastname != lname {
			t.Error(fmt.Errorf("Did not find the correct person with phone123 & lname %s", lname))
			t.Fail()
		}
		if i%batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.Fail()
			}
			t1, _ = newMockTransaction(t, sop.ForReading, -1)
			t1.Begin()
			b3, _ = NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
				Name:                     "persondb",
				SlotLength:               nodeSlotLength,
				IsUnique:                 false,
				IsValueDataInNodeSegment: false,
				LeafLoadBalancing:        false,
				Description:              "",
			}, t1, Compare)
		}
	}
}

func Test_VolumeDeletes(t *testing.T) {
	start := 9001
	end := 100000

	t1, _ := newMockTransaction(t, sop.ForWriting, -1)
	t1.Begin()
	b3, _ := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, t1, Compare)

	// Populating 90,000 items took about few minutes, did not use Kafka based delete service.
	for i := start; i <= end; i++ {
		pk, _ := newPerson("jack", fmt.Sprintf("reepper%d", i), "male", "email very very long long long", "phone123")
		if ok, err := b3.Remove(ctx, pk); !ok || err != nil {
			if err != nil {
				t.Error(err)
			}
			// Ignore not found as item could had been deleted in previous run.
		}
		if i%batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.Fail()
			}
			t1, _ = newMockTransaction(t, sop.ForWriting, -1)
			t1.Begin()
			b3, _ = NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
				Name:                     "persondb",
				SlotLength:               nodeSlotLength,
				IsUnique:                 false,
				IsValueDataInNodeSegment: false,
				LeafLoadBalancing:        false,
				Description:              "",
			}, t1, Compare)
		}
	}
}

// Mixed CRUD operations.
func Test_MixedOperations(t *testing.T) {
	start := 9000
	end := 14000

	t1, _ := newMockTransaction(t, sop.ForWriting, -1)
	t1.Begin()
	b3, _ := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, t1, Compare)

	lastNamePrefix := "zoltan"
	firstName := "jack"

	// Seed the DB with test items. And mix it up with Search/Fetch.
	for i := start; i <= end; i++ {
		pk, p := newPerson(firstName, fmt.Sprintf("%s%d", lastNamePrefix, i), "male", "email very very long long long", "phone123")
		if ok, _ := b3.AddIfNotExist(ctx, pk, p); ok {
			t.Logf("%v inserted", pk)
		}

		if i > start+100 {
			pk2, _ := newPerson(firstName, fmt.Sprintf("%s%d", lastNamePrefix, i-99), "male", "email very very long long long", "phone123")
			ok, err := b3.Find(ctx, pk2, false)
			if err != nil {
				t.Log(err)
				t.Fail()
			}
			item, _ := b3.GetCurrentItem(ctx)
			if !ok || item.Key.Firstname != pk2.Firstname || item.Key.Lastname != pk2.Lastname {
				t.Logf("Failed to find %v, found %v instead.", pk2, item.Key)
				t.Fail()
			}
		}

		if i%batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.Fail()
			}
			t1, _ = newMockTransaction(t, sop.ForWriting, -1)
			t1.Begin()
			b3, _ = NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
				Name:                     "persondb",
				SlotLength:               nodeSlotLength,
				IsUnique:                 false,
				IsValueDataInNodeSegment: false,
				LeafLoadBalancing:        false,
				Description:              "",
			}, t1, Compare)
		}
	}

	// Do Read, Delete & Update mix.
	for i := start + 100; i <= end; i++ {
		pk, p := newPerson(firstName, fmt.Sprintf("%s%d", lastNamePrefix, i), "male", "email very very long long long", "phone123")
		n := i % 3
		switch n {
		// Read on 0.
		case 0:
			if ok, _ := b3.Find(ctx, pk, false); !ok || b3.GetCurrentKey().Key.Lastname != pk.Lastname {
				t.Errorf("FindOne failed, got = %v, want = %v.", b3.GetCurrentKey(), pk)
				t.Fail()
			}
		// Delete on 1.
		case 1:
			if ok, _ := b3.Remove(ctx, pk); !ok {
				t.Errorf("Remove %v failed.", pk)
				t.Fail()
			}
		// Update on 2.
		case 2:
			if ok, _ := b3.Update(ctx, pk, p); !ok {
				t.Errorf("Update %v failed.", pk)
				t.Fail()
			}
		}

		if i%batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.Fail()
			}
			t1, _ = newMockTransaction(t, sop.ForWriting, -1)
			t1.Begin()
			b3, _ = NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
				Name:                     "persondb",
				SlotLength:               nodeSlotLength,
				IsUnique:                 false,
				IsValueDataInNodeSegment: false,
				LeafLoadBalancing:        false,
				Description:              "",
			}, t1, Compare)
		}
	}
}
