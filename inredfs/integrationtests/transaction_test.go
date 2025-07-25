package integrationtests

import (
	"cmp"
	"context"
	"fmt"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/inredfs"
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
func Compare(x PersonKey, y PersonKey) int {
	i := cmp.Compare[string](x.Lastname, y.Lastname)
	if i != 0 {
		return i
	}
	return cmp.Compare[string](x.Firstname, y.Firstname)
}

const nodeSlotLength = 500
const batchSize = 200

const tableName1 = "person2db"
const tableName2 = "twophase22"

func Test_SimpleAddPerson(t *testing.T) {
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	trans.Begin()

	pk, p := newPerson("joe", "krueger", "male", "email", "phone")

	b3, err := inredfs.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     tableName1,
		SlotLength:               nodeSlotLength,
		IsValueDataInNodeSegment: true,
	}, trans, Compare)
	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.FailNow()
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

func AddToBreakNodeThenRemoveAll(t *testing.T) {
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, _ := inredfs.NewTransaction(ctx, to)
	trans.Begin()

	pk, p := newPerson("foo", "bar", "male", "email", "phone")
	b3, _ := inredfs.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "personfoo",
		SlotLength:               2,
		IsValueDataInNodeSegment: true,
	}, trans, Compare)

	for i := 0; i < 5000; i++ {
		pk.Firstname = fmt.Sprintf("hello%d", i)
		b3.Add(ctx, pk, p)
	}

	trans.Commit(ctx)
	trans, _ = inredfs.NewTransaction(ctx, to)

	trans.Begin()
	b3, _ = inredfs.OpenBtree[PersonKey, Person](ctx, "personfoo", trans, Compare)

	for i := 0; i < 5000; i++ {
		pk.Firstname = fmt.Sprintf("hello%d", i)
		b3.Remove(ctx, pk)
	}

	trans.Commit(ctx)
}

func Test_AddThenTripleUpdatesPerson(t *testing.T) {
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	trans.Begin()

	pk, p := newPerson("foo", "bar", "male", "email", "phone")
	b3, err := inredfs.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     tableName1,
		SlotLength:               nodeSlotLength,
		IsValueDataInNodeSegment: true,
	}, trans, Compare)
	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.FailNow()
	}
	if ok, err := b3.Add(ctx, pk, p); !ok || err != nil {
		t.Errorf("Add('foo') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}

	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}

	trans, err = inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}

	trans.Begin()
	b3, err = inredfs.OpenBtree[PersonKey, Person](ctx, tableName1, trans, Compare)
	if err != nil {
		t.Errorf("Error opening Btree, details: %v.", err)
		t.FailNow()
	}
	if ok, err := b3.Update(ctx, pk, p); !ok || err != nil {
		t.Errorf("Update('foo') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		t.FailNow()
	}

	if err = trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}

	trans, err = inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}

	trans.Begin()
	b3, err = inredfs.OpenBtree[PersonKey, Person](ctx, tableName1, trans, Compare)
	if err != nil {
		t.Errorf("Error opening Btree, details: %v.", err)
		t.FailNow()
	}
	if ok, err := b3.Update(ctx, pk, p); !ok || err != nil {
		t.Errorf("Update('foo') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		t.FailNow()
	}

	if err = trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}

	trans, err = inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}

	trans.Begin()
	b3, err = inredfs.OpenBtree[PersonKey, Person](ctx, tableName1, trans, Compare)
	if err != nil {
		t.Errorf("Error opening Btree, details: %v.", err)
		t.FailNow()
	}
	if ok, err := b3.Update(ctx, pk, p); !ok || err != nil {
		t.Errorf("Update('foo') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		t.FailNow()
	}

	if err = trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}

	trans, err = inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}

	trans.Begin()
	b3, err = inredfs.OpenBtree[PersonKey, Person](ctx, tableName1, trans, Compare)
	if err != nil {
		t.Errorf("Error opening Btree, details: %v.", err)
		t.FailNow()
	}
	if ok, err := b3.Remove(ctx, pk); !ok || err != nil {
		t.Errorf("Remove('foo') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		t.FailNow()
	}

	if err = trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}

}

func Test_TwoTransactionsWithNoConflict(t *testing.T) {
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}

	trans2, _ := inredfs.NewTransaction(ctx, to)

	trans.Begin()
	trans2.Begin()

	pk, p := newPerson("tracy", "swift", "female", "email", "phone")
	b3, err := inredfs.OpenBtree[PersonKey, Person](ctx, tableName1, trans, Compare)
	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.FailNow()
	}
	if ok, err := b3.Add(ctx, pk, p); !ok || err != nil {
		t.Errorf("b3.Add('tracy') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}

	b32, err := inredfs.OpenBtree[PersonKey, Person](ctx, tableName1, trans2, Compare)
	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.FailNow()
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
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}

	trans.Begin()
	b3, err := inredfs.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     tableName1,
		SlotLength:               nodeSlotLength,
		IsValueDataInNodeSegment: true,
	}, trans, Compare)
	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.FailNow()
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
		t.Errorf("%s", err.Error())
		t.FailNow()
		return
	}

	to2, _ := inredfs.NewTransactionOptions(dataPath, sop.ForReading, -1, fs.MinimumModValue)
	trans, err = inredfs.NewTransaction(ctx, to2)
	if err != nil {
		t.Errorf("%s", err.Error())
		t.FailNow()
		return
	}

	if err := trans.Begin(); err != nil {
		t.Errorf("%s", err.Error())
		t.FailNow()
		return
	}

	b3, err = inredfs.OpenBtree[PersonKey, Person](ctx, tableName1, trans, Compare)
	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.FailNow()
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
	ctx := context.Background()
	start := 9001
	end := 100000

	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	t1, _ := inredfs.NewTransaction(ctx, to)
	t1.Begin()
	b3, _ := inredfs.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     tableName1,
		SlotLength:               nodeSlotLength,
		IsValueDataInNodeSegment: true,
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
				t.FailNow()
			}
			t1, _ = inredfs.NewTransaction(ctx, to)
			t1.Begin()
			b3, _ = inredfs.OpenBtree[PersonKey, Person](ctx, tableName1, t1, Compare)
		}
	}

	// Search them all. Searching 90,000 items just took few seconds in my laptop.
	to2, _ := inredfs.NewTransactionOptions(dataPath, sop.ForReading, -1, fs.MinimumModValue)

	b3.Last(ctx)
	b3.First(ctx)

	for i := start; i <= end; i++ {
		lname := fmt.Sprintf("reepper%d", i)
		pk, _ := newPerson("jack", lname, "male", "email very very long long long", "phone123")
		if found, err := b3.Find(ctx, pk, false); !found || err != nil {
			t.Error(err)
			t.FailNow()
		}
		ci, _ := b3.GetCurrentItem(ctx)
		if ci.Value.Phone != "phone123" || ci.Key.Lastname != lname {
			t.Error(fmt.Errorf("Did not find the correct person with phone123 & lname %s", lname))
			t.FailNow()
		}
		if i%batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.FailNow()
			}
			t1, _ = inredfs.NewTransaction(ctx, to2)
			t1.Begin()
			b3, _ = inredfs.OpenBtree[PersonKey, Person](ctx, tableName1, t1, Compare)
		}
	}
}

// Add prefix Test_ if wanting to run this test.
func Test_VolumeDeletes(t *testing.T) {
	ctx := context.Background()
	start := 9001
	end := 100000

	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	//to.UseCacheForFileRegionLocks = true
	t1, _ := inredfs.NewTransaction(ctx, to)
	t1.Begin()
	b3, _ := inredfs.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     tableName1,
		SlotLength:               nodeSlotLength,
		IsValueDataInNodeSegment: true,
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
				t.FailNow()
			}
			t1, _ = inredfs.NewTransaction(ctx, to)
			t1.Begin()
			b3, _ = inredfs.OpenBtree[PersonKey, Person](ctx, tableName1, t1, Compare)
		}
	}
}

// Mixed CRUD operations.
// Add prefix Test_ if wanting to run this test.
func Test_MixedOperations(t *testing.T) {
	ctx := context.Background()
	start := 9000
	end := 14000

	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	t1, _ := inredfs.NewTransaction(ctx, to)
	t1.Begin()
	b3, _ := inredfs.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     tableName1,
		SlotLength:               nodeSlotLength,
		IsValueDataInNodeSegment: true,
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
				t.FailNow()
			}
			item, _ := b3.GetCurrentItem(ctx)
			if !ok || item.Key.Firstname != pk2.Firstname || item.Key.Lastname != pk2.Lastname {
				t.Logf("Failed to find %v, found %v instead.", pk2, item.Key)
				t.FailNow()
			}
		}

		if i%batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.FailNow()
			}
			t1, _ = inredfs.NewTransaction(ctx, to)
			t1.Begin()
			b3, _ = inredfs.OpenBtree[PersonKey, Person](ctx, tableName1, t1, Compare)
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
				t.Errorf("FindOne failed, got = %v, want = %v.", b3.GetCurrentKey().Key, pk)
				t.FailNow()
			}
		// Delete on 1.
		case 1:
			if ok, _ := b3.Remove(ctx, pk); !ok {
				t.Errorf("Remove %v failed.", pk)
				t.FailNow()
			}
		// Update on 2.
		case 2:
			if ok, _ := b3.Update(ctx, pk, p); !ok {
				t.Errorf("Update %v failed.", pk)
				t.FailNow()
			}
		}

		if i%batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.FailNow()
			}
			t1, _ = inredfs.NewTransaction(ctx, to)
			t1.Begin()
			b3, _ = inredfs.OpenBtree[PersonKey, Person](ctx, tableName1, t1, Compare)
		}
	}
}

func Test_TwoPhaseCommitRolledback(t *testing.T) {
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	t1, _ := inredfs.NewTransaction(ctx, to)
	t1.Begin()

	b3, _ := inredfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     tableName2,
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        true,
	}, t1, nil)
	originalCount := b3.Count()
	b3.Add(ctx, 5000, "I am the value with 5000 key.")
	b3.Add(ctx, 5001, "I am the value with 5001 key.")
	b3.Add(ctx, 5000, "I am also a value with 5000 key.")
	if b3.Count() != originalCount+3 {
		t.Errorf("Count() failed, got %v, want %v", b3.Count(), originalCount+3)
	}

	twoPhase := t1.GetPhasedTransaction()

	if err := twoPhase.Phase1Commit(ctx); err == nil {
		if err = twoPhase.Rollback(ctx, nil); err != nil {
			t.Errorf("Rollback error: %v", err)
		}

		t1, _ = inredfs.NewTransaction(ctx, to)
		t1.Begin()

		b3, _ = inredfs.OpenBtree[int, string](ctx, tableName2, t1, nil)
		if b3.Count() != originalCount {
			t.Errorf("Rollback Count() failed, got %v, want %v", b3.Count(), originalCount)
		}
	} else {
		t.Errorf("No error expected, got %v", err)
	}
}
