package integration_tests

import (
	"fmt"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/SharedCode/sop/in_red_ck"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

// Covers all of these cases:
// Two transactions updating same item.
// Two transactions updating different items with collision on 1 item.
// Transaction rolls back, new completes fine.
// Reader transaction succeeds.
func Test_TwoTransactionsUpdatesOnSameItem(t *testing.T) {
	t1, err := in_red_ck.NewTransaction(true, -1)
	t2, err := in_red_ck.NewTransaction(true, -1)

	t1.Begin()
	t2.Begin()

	b3, err := in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t1)
	if err != nil {
		t.Error(err.Error()) // most likely, the "persondb77" b-tree store has not been created yet.
		t.Fail()
	}

	pk, p := newPerson("peter", "swift", "male", "email", "phone")
	pk2, p2 := newPerson("peter", "parker", "male", "email", "phone")

	found, err := b3.FindOne(ctx, pk, false)
	if !found {
		b3.Add(ctx, pk, p)
		b3.Add(ctx, pk2, p2)
		t1.Commit(ctx)
		t1, _ = in_red_ck.NewTransaction(true, -1)
		t1.Begin()
		b3, _ = in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t1)
	}

	b32, _ := in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t2)

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
	t1, _ = in_red_ck.NewTransaction(false, -1)
	t1.Begin()
	b3, _ = in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t1)
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
	t1, err := in_red_ck.NewTransaction(true, -1)
	t2, err := in_red_ck.NewTransaction(true, -1)

	t1.Begin()
	t2.Begin()

	b3, err := in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t1)
	if err != nil {
		t.Error(err.Error()) // most likely, the "persondb77" b-tree store has not been created yet.
		t.Fail()
	}

	pk, p := newPerson("joe", "pirelli", "male", "email", "phone")
	pk2, p2 := newPerson("joe2", "pirelli", "male", "email", "phone")

	found, err := b3.FindOne(ctx, pk, false)
	if !found {
		b3.Add(ctx, pk, p)
		b3.Add(ctx, pk2, p2)
		t1.Commit(ctx)
		t1, _ = in_red_ck.NewTransaction(true, -1)
		t1.Begin()
		b3, _ = in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t1)
	}

	b32, _ := in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t2)

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
	if err1 != nil {
		t.Error(err1)
	}
	if err2 != nil {
		t.Error(err2)
	}
}

// Reader transaction fails commit when an item read was modified by another transaction in-flight.
func Test_TwoTransactionsOneReadsAnotherWritesSameItem(t *testing.T) {
	t1, err := in_red_ck.NewTransaction(true, -1)
	t2, err := in_red_ck.NewTransaction(false, -1)

	t1.Begin()
	t2.Begin()

	b3, err := in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t1)
	if err != nil {
		t.Error(err.Error()) // most likely, the "persondb77" b-tree store has not been created yet.
		t.Fail()
	}

	pk, p := newPerson("joe", "zoey", "male", "email", "phone")
	pk2, p2 := newPerson("joe2", "zoey", "male", "email", "phone")

	found, err := b3.FindOne(ctx, pk, false)
	if !found {
		b3.Add(ctx, pk, p)
		b3.Add(ctx, pk2, p2)
		t1.Commit(ctx)
		t1, _ = in_red_ck.NewTransaction(true, -1)
		t1.Begin()
		b3, _ = in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t1)
	}

	b32, _ := in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t2)

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
	t1, err := in_red_ck.NewTransaction(true, -1)
	t2, err := in_red_ck.NewTransaction(false, -1)

	t1.Begin()
	t2.Begin()

	b3, err := in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t1)
	if err != nil {
		t.Error(err.Error()) // most likely, the "persondb77" b-tree store has not been created yet.
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
		t1, _ = in_red_ck.NewTransaction(true, -1)
		t1.Begin()
		b3, _ = in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t1)
	}

	b32, _ := in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t2)

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
	t1, err := in_red_ck.NewTransaction(true, -1)
	t2, err := in_red_ck.NewTransaction(true, -1)

	t1.Begin()
	t2.Begin()

	b3, err := in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t1)
	if err != nil {
		t.Error(err.Error()) // most likely, the "persondb77" b-tree store has not been created yet.
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
		t1, _ = in_red_ck.NewTransaction(true, -1)
		t1.Begin()
		b3, _ = in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t1)
	}

	b32, _ := in_red_ck.NewBtree[PersonKey, Person](ctx, "persondb77", nodeSlotLength, false, true, false, "", t2)

	b3.FindOne(ctx, pk, false)
	ci, _ := b3.GetCurrentItem(ctx)
	itemID := ci.ID
	p.SSN = "789"
	b3.UpdateCurrentItem(ctx, p)

	// Cause an update to "joe zoeyb" on t2, 'should generate conflict!
	b32.FindOneWithID(ctx, pk, itemID)
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

func Test_Concurrent2CommitsOnNewBtree(t *testing.T) {
	sr := cas.NewStoreRepository()
	sr.Remove(ctx, "twophase3")

	t1, _ := in_red_ck.NewTransaction(true, -1)
	t1.Begin()
	b3, _ := in_red_ck.NewBtree[int, string](ctx, "twophase3", 8, false, true, true, "", t1)
	// Add a single item so we persist "root node".
	b3.Add(ctx, 500, "I am the value with 500 key.")
	t1.Commit(ctx)

	eg, ctx2 := errgroup.WithContext(ctx)

	f1 := func() error {
		t1, _ := in_red_ck.NewTransaction(true, -1)
		t1.Begin()
		b3, _ := in_red_ck.NewBtree[int, string](ctx2, "twophase3", 8, false, true, true, "", t1)
		b3.Add(ctx2, 5000, "I am the value with 5000 key.")
		b3.Add(ctx2, 5001, "I am the value with 5001 key.")
		b3.Add(ctx2, 5002, "I am also a value with 5000 key.")
		return t1.Commit(ctx2)
	}

	f2 := func() error {
		t2, _ := in_red_ck.NewTransaction(true, -1)
		t2.Begin()
		b32, _ := in_red_ck.NewBtree[int, string](ctx2, "twophase3", 8, false, true, true, "", t2)
		b32.Add(ctx2, 5500, "I am the value with 5000 key.")
		b32.Add(ctx2, 5501, "I am the value with 5001 key.")
		b32.Add(ctx2, 5502, "I am also a value with 5000 key.")
		return t2.Commit(ctx2)
	}

	eg.Go(f1)
	eg.Go(f2)

	if err := eg.Wait(); err != nil {
		t.Error(err)
		return
	}

	t1, _ = in_red_ck.NewTransaction(false, -1)
	t1.Begin()

	b3, _ = in_red_ck.OpenBtree[int, string](ctx, "twophase3", t1)

	b3.First(ctx)
	i := 1
	for {
		if ok, err := b3.Next(ctx); err != nil {
			t.Error(err)
		} else if !ok {
			break
		}
		i++
	}
	if i < 6 {
		t.Errorf("Failed, traversing/counting all records, got %d, want 6.", i)
	}
}

/* 
- A commit with no conflict: commit success
- A commit with partial conflict: retry success
- A commit with full conflict: retry success
*/
func Test_ConcurrentCommitsComplexDupeAllowed(t *testing.T) {
	sr := cas.NewStoreRepository()
	sr.Remove(ctx, "tablex")

	t1, _ := in_red_ck.NewTransaction(true, -1)
	t1.Begin()
	b3, _ := in_red_ck.NewBtree[int, string](ctx, "tablex", 8, false, true, true, "", t1)
	// Add a single item so we persist "root node".
	b3.Add(ctx, 1, "I am the value with 500 key.")
	t1.Commit(ctx)

	eg, ctx2 := errgroup.WithContext(ctx)

	f1 := func() error {
		t1, _ := in_red_ck.NewTransaction(true, -1)
		t1.Begin()
		b3, _ := in_red_ck.OpenBtree[int, string](ctx2, "tablex", t1)
		b3.Add(ctx2, 50, "I am the value with 5000 key.")
		b3.Add(ctx2, 51, "I am the value with 5001 key.")
		b3.Add(ctx2, 52, "I am also a value with 5000 key.")
		return t1.Commit(ctx2)
	}

	f2 := func() error {
		t2, _ := in_red_ck.NewTransaction(true, -1)
		t2.Begin()
		b32, _ := in_red_ck.OpenBtree[int, string](ctx2, "tablex", t2)
		b32.Add(ctx2, 550, "I am the value with 5000 key.")
		b32.Add(ctx2, 551, "I am the value with 5001 key.")
		b32.Add(ctx2, 552, "I am the value with 5001 key.")
		return t2.Commit(ctx2)
	}

	f3 := func() error {
		t3, _ := in_red_ck.NewTransaction(true, -1)
		t3.Begin()
		b32, _ := in_red_ck.OpenBtree[int, string](ctx2, "tablex", t3)
		b32.Add(ctx2, 550, "random foo.")
		b32.Add(ctx2, 551, "bar hello.")
		return t3.Commit(ctx2)
	}

	eg.Go(f1)
	eg.Go(f2)
	eg.Go(f3)

	if err := eg.Wait(); err != nil {
		t.Error(err)
		return
	}

	t1, _ = in_red_ck.NewTransaction(false, -1)
	t1.Begin()

	b3, _ = in_red_ck.OpenBtree[int, string](ctx, "tablex", t1)
	b3.First(ctx)
	i := 1
	for {
		if ok, err := b3.Next(ctx); err != nil {
			t.Error(err)
		} else if !ok {
			break
		}
		i++
	}
	if i < 5 {
		t.Errorf("Failed, traversing/counting all records, got %d, want 5.", i)
	}
}

/*
- A commit with no conflict: commit success
One or both of these two should fail:
- A commit with partial conflict.
- A commit with full conflict.
*/
func Test_ConcurrentCommitsComplexDupeNotAllowed(t *testing.T) {
	sr := cas.NewStoreRepository()
	sr.Remove(ctx, "tablex2")

	t1, _ := in_red_ck.NewTransaction(true, -1)
	t1.Begin()
	b3, _ := in_red_ck.NewBtree[int, string](ctx, "tablex2", 8, true, true, true, "", t1)
	// Add a single item so we persist "root node".
	b3.Add(ctx, 1, "I am the value with 500 key.")
	t1.Commit(ctx)

	eg, ctx2 := errgroup.WithContext(ctx)

	f1 := func() error {
		t1, _ := in_red_ck.NewTransaction(true, -1)
		t1.Begin()
		b3, _ := in_red_ck.OpenBtree[int, string](ctx2, "tablex2", t1)
		b3.Add(ctx2, 50, "I am the value with 5000 key.")
		b3.Add(ctx2, 51, "I am the value with 5001 key.")
		b3.Add(ctx2, 52, "I am also a value with 5000 key.")
		return t1.Commit(ctx2)
	}

	f2 := func() error {
		t2, _ := in_red_ck.NewTransaction(true, -1)
		t2.Begin()
		b32, _ := in_red_ck.OpenBtree[int, string](ctx2, "tablex2", t2)
		b32.Add(ctx2, 550, "I am the value with 5000 key.")
		b32.Add(ctx2, 551, "I am the value with 5001 key.")
		b32.Add(ctx2, 552, "I am the value with 5001 key.")
		return t2.Commit(ctx2)
	}

	f3 := func() error {
		t3, _ := in_red_ck.NewTransaction(true, -1)
		t3.Begin()
		b32, _ := in_red_ck.OpenBtree[int, string](ctx2, "tablex2", t3)
		b32.Add(ctx2, 550, "random foo.")
		b32.Add(ctx2, 551, "bar hello.")
		return t3.Commit(ctx2)
	}

	eg.Go(f1)
	eg.Go(f2)
	eg.Go(f3)

	if err := eg.Wait(); err == nil {
		t.Error("Failed, got no error, want an error")
		return
	}

	t1, _ = in_red_ck.NewTransaction(false, -1)
	t1.Begin()

	b3, _ = in_red_ck.OpenBtree[int, string](ctx, "tablex2", t1)
	b3.First(ctx)
	i := 1
	for {
		fmt.Printf("Item with key: %v\n", b3.GetCurrentKey())
		if ok, err := b3.Next(ctx); err != nil {
			t.Error(err)
		} else if !ok {
			break
		}
		i++
	}
	if i < 3 || i > 6 {
		t.Errorf("Failed, traversing/counting all records, got %d, want 3 to 6", i)
	}
	fmt.Printf("Count of records: %d\n", i)
}

/* 
- A commit with no conflict: commit success
- A commit with partial conflict on update: rollback
- A commit with full conflict on update: rollback
*/
func Test_ConcurrentCommitsComplexUpdateConflicts(t *testing.T) {
	sr := cas.NewStoreRepository()
	sr.Remove(ctx, "tabley")

	t1, _ := in_red_ck.NewTransaction(true, -1)
	t1.Begin()
	b3, _ := in_red_ck.NewBtree[int, string](ctx, "tabley", 8, false, true, true, "", t1)
	// Add a single item so we persist "root node".
	b3.Add(ctx, 1, "I am the value with 500 key.")
	b3.Add(ctx, 550, "I am the value with 5000 key.")
	b3.Add(ctx, 551, "I am the value with 5001 key.")
	b3.Add(ctx, 552, "I am the value with 5001 key.")
	t1.Commit(ctx)

	eg, ctx2 := errgroup.WithContext(ctx)
	eg2, ctx3 := errgroup.WithContext(ctx)

	f1 := func() error {
		t1, _ := in_red_ck.NewTransaction(true, -1)
		t1.Begin()
		b3, _ := in_red_ck.OpenBtree[int, string](ctx3, "tabley", t1)
		b3.Add(ctx3, 50, "I am the value with 5000 key.")
		b3.Add(ctx3, 51, "I am the value with 5001 key.")
		b3.Add(ctx3, 52, "I am also a value with 5000 key.")
		return t1.Commit(ctx3)
	}

	f2 := func() error {
		t2, _ := in_red_ck.NewTransaction(true, -1)
		t2.Begin()
		b32, _ := in_red_ck.OpenBtree[int, string](ctx2, "tabley", t2)
		b32.Update(ctx2, 550, "I am the value with 5000 key.")
		b32.Update(ctx2, 551, "I am the value with 5001 key.")
		b32.Update(ctx2, 552, "I am the value with 5001 key.")
		return t2.Commit(ctx2)
	}

	f3 := func() error {
		t3, _ := in_red_ck.NewTransaction(true, -1)
		t3.Begin()
		b32, _ := in_red_ck.OpenBtree[int, string](ctx2, "tabley", t3)
		b32.Update(ctx2, 550, "random foo.")
		b32.Update(ctx2, 551, "bar hello.")
		return t3.Commit(ctx2)
	}

	eg2.Go(f1)
	eg.Go(f2)
	eg.Go(f3)

	if err := eg.Wait(); err == nil {
		t.Error("Failed, got no error, want an error")
		return
	}
	if err := eg2.Wait(); err != nil {
		t.Error(err)
		return
	}

	t1, _ = in_red_ck.NewTransaction(false, -1)
	t1.Begin()

	b3, _ = in_red_ck.OpenBtree[int, string](ctx, "tabley", t1)
	b3.First(ctx)
	i := 1
	for {
		fmt.Printf("Item with key: %v\n", b3.GetCurrentKey())
		if ok, err := b3.Next(ctx); err != nil {
			t.Error(err)
		} else if !ok {
			break
		}
		i++
	}
	if i != 7 {
		t.Errorf("Failed, traversing/counting all records, got %d, want 7.", i)
	}
}
