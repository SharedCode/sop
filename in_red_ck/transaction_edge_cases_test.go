package in_red_ck

import (
	"testing"
)

/*
  - Two transactions updating different items with collision on 1 item.
  - Two transactions updating different items with no collision but items' keys are sequential/contiguous between the two.
  - One transaction updates a colliding item in 1st and a 2nd trans, updates the colliding item as last.
  - Transaction rolls back, new transaction is fine.
  - Transactions roll back, new completes fine.
  - Reader transaction succeeds.
  - Reader transaction fails commit when an item read was modified by another transaction in-flight.
  - [add more test cases here...]
*/

// Two transactions updating same item.
func Test_TwoTransactionsUpdatesOnSameItem(t *testing.T) {
	t.Logf("Transaction story, single b-tree, person record test.\n")

	t1, err := NewTransaction(true, -1)
	t2, err := NewTransaction(true, -1)

	t1.Begin()
	t2.Begin()

	b3, err := OpenBtree[PersonKey, Person](ctx, "persondb", t1)
  if err != nil {
    t.Error(err.Error())  // most likely, the "persondb" b-tree store has not been created yet.
    t.Fail()
  }

  pk, p := newPerson("peter", "swift", "male", "email", "phone")
  pk2, p2 := newPerson("peter", "parker", "male", "email", "phone")

  found, err := b3.FindOne(ctx, pk, false)
  if !found {
    b3.Add(ctx, pk, p)
    b3.Add(ctx, pk2, p2)
    t1.Commit(ctx)
    t1, _ = NewTransaction(true, -1)
    t1.Begin()
    b3, _ = OpenBtree[PersonKey, Person](ctx, "persondb", t1)
  }

	b32, _ := OpenBtree[PersonKey, Person](ctx, "persondb", t2)

  // edit "peter parker" in both btrees.
  b3.FindOne(ctx, pk2, false)
  p2.SSN = "789"
  b3.UpdateCurrentItem(ctx, p2)

  b32.FindOne(ctx, pk2, false)
  p2.SSN = "xyz"
  b32.UpdateCurrentItem(ctx, p2)

  // Commit t1 & t2.
	err1 := t1.Commit(ctx)
	err2 := t2.Commit(ctx)
  if err1 == nil && err2 == nil {
    t.Error("Expecting 1 or 2 commit(s) to fail but both succeeded.")
  }
  if err1 != nil && err2 != nil {
    t.Error("Expecting 1 or 2 commit(s) to succeed but both failed.")
  }
  t1,_ = NewTransaction(false, -1)
  t1.Begin()
  b3, _ = OpenBtree[PersonKey, Person](ctx, "persondb", t1)
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

// Two transactions updating different items with no collision but items' keys are sequential/contiguous between the two.
func Test_TwoTransactionsUpdatesOnSameNodeDifferentItems(t *testing.T) {
	t.Logf("Transaction story, single b-tree, person record test.\n")

	t1, err := NewTransaction(true, -1)
	t2, err := NewTransaction(true, -1)

	t1.Begin()
	t2.Begin()

	b3, err := OpenBtree[PersonKey, Person](ctx, "persondb", t1)
  if err != nil {
    t.Error(err.Error())  // most likely, the "persondb" b-tree store has not been created yet.
    t.Fail()
  }

  pk, p := newPerson("joe", "pirelli", "male", "email", "phone")
  pk2, p2 := newPerson("joe2", "pirelli", "male", "email", "phone")

  found, err := b3.FindOne(ctx, pk, false)
  if !found {
    b3.Add(ctx, pk, p)
    b3.Add(ctx, pk2, p2)
    t1.Commit(ctx)
    t1, _ = NewTransaction(true, -1)
    t1.Begin()
    b3, _ = OpenBtree[PersonKey, Person](ctx, "persondb", t1)
  }

	b32, _ := OpenBtree[PersonKey, Person](ctx, "persondb", t2)

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
