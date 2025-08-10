//go:build stresstests
// +build stresstests

package common

import (
	"fmt"
	"testing"

	"github.com/sharedcode/sop"
)

// Heavy/flaky transaction scenarios extracted to run under the 'stresstests' build tag.

func TestStress_TwoTransactionsUpdatesOnSameItem(t *testing.T) {
	t1, _ := newMockTransaction(t, sop.ForWriting, -1)
	t2, _ := newMockTransaction(t, sop.ForWriting, -1)

	t1.Begin()
	t2.Begin()

	b3, err := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, t1, Compare)

	if err != nil {
		t.Error(err.Error())
		t.Fail()
	}

	pk, p := newPerson("peter", "swift", "male", "email", "phone")
	pk2, p2 := newPerson("peter", "parker", "male", "email", "phone")

	found, _ := b3.Find(ctx, pk, false)
	if !found {
		b3.Add(ctx, pk, p)
		b3.Add(ctx, pk2, p2)
		t1.Commit(ctx)
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

	b32, _ := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, t2, Compare)

	// edit "peter parker" in both btrees.
	pk3, p3 := newPerson("gokue", "kakarot", "male", "email", "phone")
	b3.Add(ctx, pk3, p3)
	b3.Find(ctx, pk2, false)
	p2.SSN = "789"
	b3.UpdateCurrentItem(ctx, p2)

	b32.Find(ctx, pk2, false)
	p2.SSN = "xyz"
	b32.UpdateCurrentItem(ctx, p2)

	// Commit t1 & t2.
	_ = t1.Commit(ctx)
	_ = t2.Commit(ctx)
}

func TestStress_TwoTransactionsUpdatesOnSameNodeDifferentItems(t *testing.T) {
	t1, _ := newMockTransaction(t, sop.ForWriting, -1)
	t2, _ := newMockTransaction(t, sop.ForWriting, -1)

	t1.Begin()
	t2.Begin()

	b3, err := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, t1, Compare)
	if err != nil {
		t.Error(err.Error())
		t.Fail()
	}

	pk, p := newPerson("joe", "pirelli", "male", "email", "phone")
	pk2, p2 := newPerson("joe2", "pirelli", "male", "email", "phone")

	found, _ := b3.Find(ctx, pk, false)
	if !found {
		b3.Add(ctx, pk, p)
		b3.Add(ctx, pk2, p2)
		t1.Commit(ctx)
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

	b32, _ := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "persondb",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, t2, Compare)

	// edit both "pirellis" in both btrees, one each.
	b3.Find(ctx, pk, false)
	p.SSN = "789"
	b3.UpdateCurrentItem(ctx, p)

	b32.Find(ctx, pk2, false)
	p2.SSN = "abc"
	b32.UpdateCurrentItem(ctx, p2)

	_ = t1.Commit(ctx)
	_ = t2.Commit(ctx)
}

func TestStress_AddAndSearchManyPersons(t *testing.T) {
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
}
