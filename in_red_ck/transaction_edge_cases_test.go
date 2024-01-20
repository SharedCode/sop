package in_red_ck

import (
	"testing"

	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

// Covers all of these cases:
// Two transactions updating same item.
// Two transactions updating different items with collision on 1 item.
// Transaction rolls back, new completes fine.
// Reader transaction succeeds.
func Test_TwoTransactionsUpdatesOnSameItem(t *testing.T) {
	t1, err := newMockTransaction(true, -1)
	t2, err := newMockTransaction(true, -1)

	t1.Begin()
	t2.Begin()

	b3, err := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
	if err != nil {
		t.Error(err.Error()) // most likely, the "persondb" b-tree store has not been created yet.
		t.Fail()
	}

	pk, p := newPerson("peter", "swift", "male", "email", "phone")
	pk2, p2 := newPerson("peter", "parker", "male", "email", "phone")

	found, err := b3.FindOne(ctx, pk, false)
	if !found {
		b3.Add(ctx, pk, p)
		b3.Add(ctx, pk2, p2)
		t1.Commit(ctx)
		t1, _ = newMockTransaction(true, -1)
		t1.Begin()
		b3, _ = NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
	}

	b32, _ := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t2)

	// edit "peter parker" in both btrees.
	pk3, p3 := newPerson("gokue", "kakarot", "male", "email", "phone")
	b3.Add(ctx, pk3, p3)
	b3.FindOne(ctx, pk2, false)
	p2.SSN = "789"
	b3.UpdateCurrentItem(ctx, p2)

	b32.FindOne(ctx, pk2, false)
	p2.SSN = "xyz"
	b32.UpdateCurrentItem(ctx, p2)

	// Commit t1 & t2.
	err1 := t1.Commit(ctx)
	err2 := t2.Commit(ctx)
	if err1 != nil {
		t.Error("Commit #1, got = fail, want = success.")
	}
	if err2 == nil {
		t.Error("Commit #2, got = succeess, want = fail.")
	}
	t1, _ = newMockTransaction(false, -1)
	t1.Begin()
	b3, _ = NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
	var person Person
	b3.FindOne(ctx, pk2, false)
	person, _ = b3.GetCurrentValue(ctx)
	if err1 == nil {
		if person.SSN != "789" {
			t.Errorf("Got SSN = %s, want = 789", person.SSN)
		}
	}
	if err2 == nil {
		if person.SSN != "xyz" {
			t.Errorf("Got SSN = %s, want = xyz", person.SSN)
		}
	}
	if err := t1.Commit(ctx); err != nil {
		t.Error(err.Error())
	}
}

// Two transactions updating different items with no collision but items'
// keys are sequential/contiguous between the two.
func Test_TwoTransactionsUpdatesOnSameNodeDifferentItems(t *testing.T) {
	t1, err := newMockTransaction(true, -1)
	t2, err := newMockTransaction(true, -1)

	t1.Begin()
	t2.Begin()

	b3, err := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
	if err != nil {
		t.Error(err.Error()) // most likely, the "persondb" b-tree store has not been created yet.
		t.Fail()
	}

	pk, p := newPerson("joe", "pirelli", "male", "email", "phone")
	pk2, p2 := newPerson("joe2", "pirelli", "male", "email", "phone")

	found, err := b3.FindOne(ctx, pk, false)
	if !found {
		b3.Add(ctx, pk, p)
		b3.Add(ctx, pk2, p2)
		t1.Commit(ctx)
		t1, _ = newMockTransaction(true, -1)
		t1.Begin()
		b3, _ = NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
	}

	b32, _ := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t2)

	// edit both "pirellis" in both btrees, one each.
	b3.FindOne(ctx, pk, false)
	p.SSN = "789"
	b3.UpdateCurrentItem(ctx, p)

	b32.FindOne(ctx, pk2, false)
	p2.SSN = "abc"
	b32.UpdateCurrentItem(ctx, p2)

	// Commit t1 & t2.
	err1 := t1.Commit(ctx)
	err2 := t2.Commit(ctx)
	if err1 != nil || err2 != nil {
		t.Error("got = commit failure, want = both commit success.")
	}
}

// Reader transaction fails commit when an item read was modified by another transaction in-flight.
func Test_TwoTransactionsOneReadsAnotherWritesSameItem(t *testing.T) {
	t1, err := newMockTransaction(true, -1)
	t2, err := newMockTransaction(false, -1)

	t1.Begin()
	t2.Begin()

	b3, err := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
	if err != nil {
		t.Error(err.Error()) // most likely, the "persondb" b-tree store has not been created yet.
		t.Fail()
	}

	pk, p := newPerson("joe", "zoey", "male", "email", "phone")
	pk2, p2 := newPerson("joe2", "zoey", "male", "email", "phone")

	found, err := b3.FindOne(ctx, pk, false)
	if !found {
		b3.Add(ctx, pk, p)
		b3.Add(ctx, pk2, p2)
		t1.Commit(ctx)
		t1, _ = newMockTransaction(true, -1)
		t1.Begin()
		b3, _ = NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
	}

	b32, _ := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t2)

	// Read both records.
	b32.FindOne(ctx, pk2, false)
	b32.GetCurrentValue(ctx)
	b32.FindOne(ctx, pk, false)
	b32.GetCurrentValue(ctx)

	// update one of the two records read on the reader transaction.
	b3.FindOne(ctx, pk, false)
	p.SSN = "789"
	b3.UpdateCurrentItem(ctx, p)

	// Commit t1 & t2.
	if err := t1.Commit(ctx); err != nil {
		t.Errorf("t1 writer Commit got error, want success, details: %v.", err)
	}
	if err := t2.Commit(ctx); err == nil {
		t.Errorf("t2 reader Commit got success, want error.")
	}
}

// Node merging and row(or item) level conflict detection.
// Case: Reader transaction succeeds commit, while another item in same Node got updated by another transaction.
func Test_TwoTransactionsOneReadsAnotherWritesAnotherItemOnSameNode(t *testing.T) {
	t1, err := newMockTransaction(true, -1)
	t2, err := newMockTransaction(false, -1)

	t1.Begin()
	t2.Begin()

	b3, err := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
	if err != nil {
		t.Error(err.Error()) // most likely, the "persondb" b-tree store has not been created yet.
		t.Fail()
	}

	pk, p := newPerson("joe", "zoeya", "male", "email", "phone")
	pk2, p2 := newPerson("joe2", "zoeya", "male", "email", "phone")
	pk3, p3 := newPerson("joe3", "zoeya", "male", "email", "phone")

	found, err := b3.FindOne(ctx, pk, false)
	if !found {
		b3.Add(ctx, pk, p)
		b3.Add(ctx, pk2, p2)
		b3.Add(ctx, pk3, p3)
		t1.Commit(ctx)
		t1, _ = newMockTransaction(true, -1)
		t1.Begin()
		b3, _ = NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
	}

	b32, _ := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t2)

	// Read both records.
	b32.FindOne(ctx, pk2, false)
	b32.GetCurrentValue(ctx)
	b32.FindOne(ctx, pk, false)
	b32.GetCurrentValue(ctx)

	// update item #3 that should be on same node.
	b3.FindOne(ctx, pk3, false)
	p.SSN = "789"
	b3.UpdateCurrentItem(ctx, p)

	// Commit t1 & t2.
	if err := t1.Commit(ctx); err != nil {
		t.Errorf("t1 writer Commit got error, want success, details: %v.", err)
	}
	if err := t2.Commit(ctx); err != nil {
		t.Errorf("t2 reader Commit got error, want success, details: %v.", err)
	}
}

// One transaction updates a colliding item in 1st and a 2nd trans.
func Test_TwoTransactionsOneUpdateItemOneAnotherUpdateItemLast(t *testing.T) {
	t1, err := newMockTransaction(true, -1)
	t2, err := newMockTransaction(true, -1)

	t1.Begin()
	t2.Begin()

	b3, err := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
	if err != nil {
		t.Error(err.Error()) // most likely, the "persondb" b-tree store has not been created yet.
		t.Fail()
	}

	pk, p := newPerson("joe", "zoeyb", "male", "email", "phone")
	pk2, p2 := newPerson("joe2", "zoeyb", "male", "email", "phone")
	pk3, p3 := newPerson("joe3", "zoeyb", "male", "email", "phone")
	pk4, p4 := newPerson("joe4", "zoeyb", "male", "email", "phone")
	pk5, p5 := newPerson("joe5", "zoeyb", "male", "email", "phone")

	found, err := b3.FindOne(ctx, pk, false)
	if !found {
		b3.Add(ctx, pk, p)
		b3.Add(ctx, pk2, p2)
		b3.Add(ctx, pk3, p3)
		b3.Add(ctx, pk4, p4)
		b3.Add(ctx, pk5, p5)
		t1.Commit(ctx)
		t1, _ = newMockTransaction(true, -1)
		t1.Begin()
		b3, _ = NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
	}

	b32, _ := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t2)

	b3.FindOne(ctx, pk, false)
	ci, _ := b3.GetCurrentItem(ctx)
	itemId := ci.Id
	p.SSN = "789"
	b3.UpdateCurrentItem(ctx, p)

	// Cause an update to "joe zoeyb" on t2, 'should generate conflict!
	b32.FindOneWithId(ctx, pk, itemId)
	p.SSN = "555"
	b32.UpdateCurrentItem(ctx, p)

	b3.FindOne(ctx, pk2, false)
	b3.GetCurrentValue(ctx)
	b3.FindOne(ctx, pk3, false)
	b3.GetCurrentValue(ctx)
	b3.FindOne(ctx, pk4, false)
	b3.GetCurrentValue(ctx)
	b3.FindOne(ctx, pk5, false)
	b3.GetCurrentValue(ctx)

	b32.FindOne(ctx, pk5, false)
	p.SSN = "789"
	b32.UpdateCurrentItem(ctx, p)

	b32.FindOne(ctx, pk4, false)
	b32.GetCurrentValue(ctx)
	b32.FindOne(ctx, pk3, false)
	b32.GetCurrentValue(ctx)
	b32.FindOne(ctx, pk2, false)
	b32.GetCurrentValue(ctx)

	// Commit t1 & t2.
	err1 := t1.Commit(ctx)
	err2 := t2.Commit(ctx)

	if err1 == nil && err2 == nil {
		t.Errorf("T1 & T2 Commits got 2 success, want 1 fail.")
	}
	if err1 != nil {
		t.Logf(err1.Error())
	}
	if err2 != nil {
		t.Logf(err2.Error())
	}
}

func Test_CommitThrowsException(t *testing.T) {
	// Commit successfully 1st so we can create a good data set that we can check if restored on commit failed.
	trans, _ := newMockTransaction(true, -1)
	trans.Begin()
	b3, _ := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", trans)
	pk, p := newPerson("joe", "zhroeger", "male", "email", "phone")
	b3.Add(ctx, pk, p)
	trans.Commit(ctx)

	// Preserve the good, nicely populated repositories.
	t2 := trans.GetPhasedTransaction().(*transaction)

	goodStoreRepository := t2.storeRepository
	goodRegistry := t2.registry
	goodRedisCache := t2.redisCache
	goodBlobStore := t2.nodeBlobStore

	trans, _ = newMockTransaction(true, -1)
	t2 = trans.GetPhasedTransaction().(*transaction)

	// Restore the populated repos.
	t2.storeRepository = goodStoreRepository
	t2.redisCache = goodRedisCache
	t2.nodeBlobStore = goodBlobStore

	// Create an update & a Commit that fails. Pass true param to Mock Registry will induce error on Commit.
	t2.registry = cas.NewMockRegistry(true)
	t2.registry.(*cas.Mock_vid_registry).Lookup = goodRegistry.(*cas.Mock_vid_registry).Lookup

	trans.Begin()
	b3, _ = OpenBtree[PersonKey, Person](ctx, "persondb", trans)

	pk, p = newPerson("joe", "zhroeger", "male2", "email2", "phone2")
	b3.Add(ctx, pk, p)

	if err := trans.Commit(ctx); err == nil {
		t.Error("Expected Commit to fail, but succeeded.")
	}

	// Capture the repos' state which we will check for validity.
	goodStoreRepository = t2.storeRepository
	goodRegistry = t2.registry
	goodRegistry.(*cas.Mock_vid_registry).InducedErrorOnUpdateAllOrNothing = false
	goodRedisCache = t2.redisCache
	goodBlobStore = t2.nodeBlobStore

	trans, _ = newMockTransaction(false, -1)
	t2 = trans.GetPhasedTransaction().(*transaction)
	t2.storeRepository = goodStoreRepository
	t2.registry = goodRegistry
	t2.redisCache = goodRedisCache
	t2.nodeBlobStore = goodBlobStore

	trans.Begin()
	b3, _ = OpenBtree[PersonKey, Person](ctx, "persondb", trans)
	if ok, _ := b3.FindOne(ctx, pk, false); !ok {
		t.Errorf("FindOne(%v) failed, got 'not found', want 'found'.", pk)
		t.Fail()
	}
	if v, _ := b3.GetCurrentValue(ctx); v.Email != "email" || v.Phone != "phone" {
		t.Errorf("GetCurrentValue failed, got (%s, %s), want ('email', 'phone').", v.Email, v.Phone)
		t.Fail()
	}
	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit failed, error got %v, want nil.", err)
	}
}
