package value_data_segment

import (
	"cmp"
	"fmt"
	"testing"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/fs"
	"github.com/SharedCode/sop/in_red_fs"
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

func Test_SimpleAddPerson(t *testing.T) {
	to, _ := in_red_fs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := in_red_fs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()

	pk, p := newPerson("joe", "krueger", "male", "email", "phone")

	b3, err := in_red_fs.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
	}, trans, Compare)
	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}
	if ok, err := b3.Add(ctx, pk, p); !ok || err != nil {
		t.Errorf("Add('joe') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}

	if ok, err := b3.FindOne(ctx, pk, false); !ok || err != nil {
		t.Errorf("FindOne('joe',false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}
	if k := b3.GetCurrentKey(); k.Firstname != pk.Firstname {
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

func Test_TwoTransactionsWithNoConflict(t *testing.T) {
	to, _ := in_red_fs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := in_red_fs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}

	trans2, err := in_red_fs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}

	trans.Begin()
	trans2.Begin()

	pk, p := newPerson("tracy", "swift", "female", "email", "phone")
	b3, err := in_red_fs.OpenBtree[PersonKey, Person](ctx, "persondb", trans, Compare)
	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}
	if ok, err := b3.Add(ctx, pk, p); !ok || err != nil {
		t.Errorf("b3.Add('tracy') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}

	b32, err := in_red_fs.OpenBtree[PersonKey, Person](ctx, "persondb", trans2, Compare)
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
	to, _ := in_red_fs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := in_red_fs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}

	trans.Begin()
	b3, err := in_red_fs.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
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

	tor, _ := in_red_fs.NewTransactionOptions(dataPath, sop.ForReading, -1, fs.MinimumModValue)
	trans, err = in_red_fs.NewTransaction(ctx, tor)
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

	b3, err = in_red_fs.OpenBtree[PersonKey, Person](ctx, "persondb", trans, Compare)
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

	to, _ := in_red_fs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	t1, _ := in_red_fs.NewTransaction(ctx, to)
	t1.Begin()
	b3, _ := in_red_fs.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:       "persondb",
		SlotLength: nodeSlotLength,
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
			t1, _ = in_red_fs.NewTransaction(ctx, to)
			t1.Begin()
			b3, _ = in_red_fs.OpenBtree[PersonKey, Person](ctx, "persondb", t1, Compare)
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
		if i%batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.Fail()
			}
			tor, _ := in_red_fs.NewTransactionOptions(dataPath, sop.ForReading, -1, fs.MinimumModValue)
			t1, _ = in_red_fs.NewTransaction(ctx, tor)
			t1.Begin()
			b3, _ = in_red_fs.OpenBtree[PersonKey, Person](ctx, "persondb", t1, Compare)
		}
	}
}

// Add prefix Test_ if wanting to run this test.
func VolumeDeletes(t *testing.T) {
	start := 9001
	end := 100000

	to, _ := in_red_fs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	t1, _ := in_red_fs.NewTransaction(ctx, to)
	t1.Begin()
	b3, _ := in_red_fs.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:       "persondb",
		SlotLength: nodeSlotLength,
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
			t1, _ = in_red_fs.NewTransaction(ctx, to)
			t1.Begin()
			b3, _ = in_red_fs.OpenBtree[PersonKey, Person](ctx, "persondb", t1, Compare)
		}
	}
}

// Mixed CRUD operations.
// Add prefix Test_ if wanting to run this test.
func MixedOperations(t *testing.T) {
	start := 9000
	end := 14000

	to, _ := in_red_fs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	t1, _ := in_red_fs.NewTransaction(ctx, to)
	t1.Begin()
	b3, _ := in_red_fs.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
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
			ok, err := b3.FindOne(ctx, pk2, false)
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
			t1, _ = in_red_fs.NewTransaction(ctx, to)
			t1.Begin()
			b3, _ = in_red_fs.OpenBtree[PersonKey, Person](ctx, "persondb", t1, Compare)
		}
	}

	// Do Read, Delete & Update mix.
	for i := start + 100; i <= end; i++ {
		pk, p := newPerson(firstName, fmt.Sprintf("%s%d", lastNamePrefix, i), "male", "email very very long long long", "phone123")
		n := i % 3
		switch n {
		// Read on 0.
		case 0:
			if ok, _ := b3.FindOne(ctx, pk, false); !ok || b3.GetCurrentKey().Lastname != pk.Lastname {
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
			t1, _ = in_red_fs.NewTransaction(ctx, to)
			t1.Begin()
			b3, _ = in_red_fs.OpenBtree[PersonKey, Person](ctx, "persondb", t1, Compare)
		}
	}
}

func Test_TwoPhaseCommitRolledback(t *testing.T) {
	to, _ := in_red_fs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	t1, _ := in_red_fs.NewTransaction(ctx, to)
	t1.Begin()

	b3, _ := in_red_fs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:              "twophase",
		SlotLength:        8,
		LeafLoadBalancing: true,
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
		twoPhase.Rollback(ctx, nil)

		t1, _ = in_red_fs.NewTransaction(ctx, to)
		t1.Begin()

		b3, _ = in_red_fs.OpenBtree[int, string](ctx, "twophase", t1, nil)
		if b3.Count() != originalCount {
			t.Errorf("Rollback Count() failed, got %v, want %v", b3.Count(), originalCount)
		}
	} else {
		t.Errorf("No error expected, got %v", err)
	}
}

func Test_StrangeBtreeStoreName(t *testing.T) {
	to, _ := in_red_fs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)

	in_red_fs.RemoveBtree(ctx, dataPath, "2phase")

	t1, _ := in_red_fs.NewTransaction(ctx, to)
	t1.Begin()

	if _, err := in_red_fs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:       "2phase",
		SlotLength: 8,
	}, t1, nil); err != nil {
		t.Error("NewBtree('2phase') failed, got err, want nil.")
	}
}
