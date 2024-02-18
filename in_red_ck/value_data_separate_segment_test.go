package in_red_ck

import (
	"fmt"
	"testing"
)

func Test_ValueDataInSeparateSegment_Rollback(t *testing.T) {
	trans, _ := NewMockTransaction(t, true, -1)
	trans.Begin()

	b3, _ := NewBtree[PersonKey, Person](ctx, StoreInfo{
		Name: "persondb7",
		SlotLength: nodeSlotLength,
		IsUnique: false, 
		IsValueDataInNodeSegment: false, 
		IsValueDataActivelyPersisted: false,
		IsValueDataGloballyCached: true,
		LeafLoadBalancing: true,
		Description: "",
	}, trans)

	pk, p := newPerson("joe", "shroeger", "male", "email", "phone")
	b3.Add(ctx, pk, p)

	trans.Commit(ctx)

	trans, _ = NewMockTransaction(t, true, -1)
	trans.Begin()

	pk, p = newPerson("joe", "shroeger", "male", "email2", "phone2")
	b3.Update(ctx, pk, p)

	trans.Rollback(ctx)

	trans, _ = NewMockTransaction(t, false, -1)
	trans.Begin()
	b3, _ = OpenBtree[PersonKey, Person](ctx, "persondb7", trans)
	pk, p = newPerson("joe", "shroeger", "male", "email", "phone")

	b3.FindOne(ctx, pk, false)
	v, _ := b3.GetCurrentValue(ctx)

	if v.Email != "email" {
		t.Errorf("Rollback did not restore person record, email got = %s, want = 'email'.", v.Email)
	}
	trans.Commit(ctx)
}

func Test_ValueDataInSeparateSegment_SimpleAddPerson(t *testing.T) {
	trans, err := NewMockTransaction(t, true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()

	pk, p := newPerson("joe", "krueger", "male", "email", "phone")

	b3, err := NewBtree[PersonKey, Person](ctx, StoreInfo{
		Name: "persondb7",
		SlotLength: nodeSlotLength,
		IsUnique: false, 
		IsValueDataInNodeSegment: false, 
		IsValueDataActivelyPersisted: false,
		IsValueDataGloballyCached: true,
		LeafLoadBalancing: true,
		Description: "",
	}, trans)
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

func Test_ValueDataInSeparateSegment_TwoTransactionsWithNoConflict(t *testing.T) {
	trans, err := NewMockTransaction(t, true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}

	trans2, err := NewMockTransaction(t, true, -1)

	trans.Begin()
	trans2.Begin()

	pk, p := newPerson("tracy", "swift", "female", "email", "phone")
	b3, err := NewBtree[PersonKey, Person](ctx, StoreInfo{
		Name: "persondb7",
		SlotLength: nodeSlotLength,
		IsUnique: false, 
		IsValueDataInNodeSegment: false, 
		IsValueDataActivelyPersisted: false,
		IsValueDataGloballyCached: true,
		LeafLoadBalancing: true,
		Description: "",
	}, trans)
	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}
	if ok, err := b3.Add(ctx, pk, p); !ok || err != nil {
		t.Errorf("b3.Add('tracy') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}

	b32, err := NewBtree[PersonKey, Person](ctx, StoreInfo{
		Name: "persondb7",
		SlotLength: nodeSlotLength,
		IsUnique: false, 
		IsValueDataInNodeSegment: false, 
		IsValueDataActivelyPersisted: false,
		IsValueDataGloballyCached: true,
		LeafLoadBalancing: true,
		Description: "",
	}, trans)
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

func Test_ValueDataInSeparateSegment_AddAndSearchManyPersons(t *testing.T) {
	trans, err := NewMockTransaction(t, true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}

	trans.Begin()
	b3, err := NewBtree[PersonKey, Person](ctx, StoreInfo{
		Name: "persondb7",
		SlotLength: nodeSlotLength,
		IsUnique: false, 
		IsValueDataInNodeSegment: false, 
		IsValueDataActivelyPersisted: false,
		IsValueDataGloballyCached: true,
		LeafLoadBalancing: true,
		Description: "",
	}, trans)
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
		t.Errorf(err.Error())
		t.Fail()
		return
	}

	trans, err = NewMockTransaction(t, false, -1)
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

	b3, err = OpenBtree[PersonKey, Person](ctx, "persondb7", trans)
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

func Test_ValueDataInSeparateSegment_VolumeAddThenSearch(t *testing.T) {
	start := 9001
	end := 15000

	t1, _ := NewMockTransaction(t, true, -1)
	t1.Begin()
	b3, _ := NewBtree[PersonKey, Person](ctx, StoreInfo{
		Name: "persondb7",
		SlotLength: nodeSlotLength,
		IsUnique: false, 
		IsValueDataInNodeSegment: false, 
		IsValueDataActivelyPersisted: false,
		IsValueDataGloballyCached: true,
		LeafLoadBalancing: true,
		Description: "",
	}, t1)

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
			t1, _ = NewMockTransaction(t, true, -1)
			t1.Begin()
			b3, _ = OpenBtree[PersonKey, Person](ctx, "persondb7", t1)
		}
	}

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
			t1, _ = NewMockTransaction(t, false, -1)
			t1.Begin()
			b3, _ = OpenBtree[PersonKey, Person](ctx, "persondb7", t1)
		}
	}
}

func Test_ValueDataInSeparateSegment_VolumeDeletes(t *testing.T) {
	start := 9001
	end := 10000

	t1, _ := NewMockTransaction(t, true, -1)
	t1.Begin()
	b3, _ := NewBtree[PersonKey, Person](ctx, StoreInfo{
		Name: "persondb7",
		SlotLength: nodeSlotLength,
		IsUnique: false, 
		IsValueDataInNodeSegment: false, 
		IsValueDataActivelyPersisted: false,
		IsValueDataGloballyCached: true,
		LeafLoadBalancing: true,
		Description: "",
	}, t1)

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
			t1, _ = NewMockTransaction(t, true, -1)
			t1.Begin()
			b3, _ = OpenBtree[PersonKey, Person](ctx, "persondb7", t1)
		}
	}
}

// Mixed CRUD operations.
func Test_ValueDataInSeparateSegment_MixedOperations(t *testing.T) {
	start := 9000
	end := 9500

	t1, _ := NewMockTransaction(t, true, -1)
	t1.Begin()
	b3, _ := NewBtree[PersonKey, Person](ctx, StoreInfo{
		Name: "persondb7",
		SlotLength: nodeSlotLength,
		IsUnique: false, 
		IsValueDataInNodeSegment: false, 
		IsValueDataActivelyPersisted: false,
		IsValueDataGloballyCached: true,
		LeafLoadBalancing: true,
		Description: "",
	}, t1)

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
			t1, _ = NewMockTransaction(t, true, -1)
			t1.Begin()
			b3, _ = OpenBtree[PersonKey, Person](ctx, "persondb7", t1)
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
			t1, _ = NewMockTransaction(t, true, -1)
			t1.Begin()
			b3, _ = OpenBtree[PersonKey, Person](ctx, "persondb7", t1)
		}
	}
}
