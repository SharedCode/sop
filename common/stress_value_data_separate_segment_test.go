//go:build stresstests
// +build stresstests

package common

import (
	"fmt"
	"testing"

	"github.com/sharedcode/sop"
)

// These are heavy/flaky scenarios extracted for opt-in runs.
// They mirror tests in value_data_separate_segment_test.go but without skips.

func TestStress_ValueDataInSeparateSegment_TwoTransactionsWithNoConflict(t *testing.T) {
	trans, err := newMockTransaction(t, sop.ForWriting, -1)
	if err != nil {
		t.Fatal(err.Error())
	}

	// Prepare second transaction handle but don't begin yet; we'll run it after committing the first.
	trans2, _ := newMockTransaction(t, sop.ForWriting, -1)

	trans.Begin()

	pk, p := newPerson("tracy", "swift", "female", "email", "phone")
	b3, err := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                         "persondb7",
		SlotLength:                   nodeSlotLength,
		IsUnique:                     false,
		IsValueDataInNodeSegment:     false,
		IsValueDataActivelyPersisted: false,
		IsValueDataGloballyCached:    true,
		LeafLoadBalancing:            true,
		Description:                  "",
	}, trans, Compare)
	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}
	if ok, err := b3.Add(ctx, pk, p); !ok || err != nil {
		t.Errorf("b3.Add('tracy') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}

	// Commit the first transaction before starting the second to avoid in-process concurrent writer instability.
	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit 1 returned error, details: %v.", err)
	}

	// Now begin and use the second writer transaction with a different key.
	trans2.Begin()

	b32, err := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                         "persondb7",
		SlotLength:                   nodeSlotLength,
		IsUnique:                     false,
		IsValueDataInNodeSegment:     false,
		IsValueDataActivelyPersisted: false,
		IsValueDataGloballyCached:    true,
		LeafLoadBalancing:            true,
		Description:                  "",
	}, trans2, Compare)
	if err != nil {
		t.Errorf("Error instantiating Btree, details: %v.", err)
		t.Fail()
	}
	pk2, p2 := newPerson("tracy2", "swift", "female", "email", "phone")
	if ok, err := b32.Add(ctx, pk2, p2); !ok || err != nil {
		t.Errorf("b32.Add('tracy2') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}

	if err := trans2.Commit(ctx); err != nil {
		t.Errorf("Commit 2 returned error, details: %v.", err)
	}
}

func TestStress_ValueDataInSeparateSegment_AddAndSearchManyPersons(t *testing.T) {
	trans, err := newMockTransaction(t, sop.ForWriting, -1)
	if err != nil {
		t.Fatal(err.Error())
	}

	trans.Begin()
	b3, err := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                         "persondb7",
		SlotLength:                   nodeSlotLength,
		IsUnique:                     false,
		IsValueDataInNodeSegment:     false,
		IsValueDataActivelyPersisted: false,
		IsValueDataGloballyCached:    true,
		LeafLoadBalancing:            true,
		Description:                  "",
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

	b3, err = OpenBtree[PersonKey, Person](ctx, "persondb7", trans, Compare)
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
