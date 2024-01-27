package in_red_ck

import (
	"fmt"
	"testing"
)

func Test_TwoPhaseCommitRolledback(t *testing.T) {
	t1, _ := newMockTransaction(t, true, -1)
	t1.Begin()

	b3, _ := NewBtree[int, string](ctx, "2phase", 8, false, true, true, "", t1)
	b3.Add(ctx, 5000, "I am the value with 5000 key.")
	b3.Add(ctx, 5001, "I am the value with 5001 key.")
	b3.Add(ctx, 5000, "I am also a value with 5000 key.")

	twoPhase := t1.GetPhasedTransaction()

	if err := twoPhase.Phase1Commit(ctx); err == nil {
		if err2 := my3rdPartyDBlogic(true); err2 != nil {
			twoPhase.Rollback(ctx)
			return
		}
		t.Error("Should not go here.")
	} else {
		t.Error("Should not go here.")
	}
}

func Test_TwoPhaseCommitCommitted(t *testing.T) {
	t1, _ := newMockTransaction(t, true, -1)
	t1.Begin()

	b3, _ := NewBtree[int, string](ctx, "2phase1", 8, false, true, true, "", t1)
	b3.Add(ctx, 5000, "I am the value with 5000 key.")
	b3.Add(ctx, 5001, "I am the value with 5001 key.")
	b3.Add(ctx, 5000, "I am also a value with 5000 key.")

	twoPhase := t1.GetPhasedTransaction()

	if err := twoPhase.Phase1Commit(ctx); err == nil {
		if err2 := my3rdPartyDBlogic(false); err2 != nil {
			t.Error("Should not go here.")
			return
		}
		twoPhase.Phase2Commit(ctx)

		t1, _ = newMockTransaction(t, false, -1)
		t1.Begin()
		b3, _ = OpenBtree[int, string](ctx, "2phase1", t1)
		twoPhase = t1.GetPhasedTransaction()

		if ok, _ := b3.FindOne(ctx, 5000, true); !ok || b3.GetCurrentKey() != 5000 {
			t.Errorf("FindOne(5000, true) failed, got = %v, want = 5000", b3.GetCurrentKey())
		}
		if ok, _ := b3.Next(ctx); !ok || b3.GetCurrentKey() != 5000 {
			t.Errorf("Next() failed, got = %v, want = 5000", b3.GetCurrentKey())
		}
		if ok, _ := b3.Next(ctx); !ok || b3.GetCurrentKey() != 5001 {
			t.Errorf("Next() failed, got = %v, want = 5001", b3.GetCurrentKey())
		}
		// Call the two phase committers just for demo, but t1.Commit(..) will work fine too.
		if err = twoPhase.Phase1Commit(ctx); err != nil {
			t.Error(err)
		}
		if err = twoPhase.Phase2Commit(ctx); err != nil {
			t.Error(err)
		}
	} else {
		t.Error("Should not go here.")
	}
}

func Test_TwoPhaseCommitRolledbackThenCommitted(t *testing.T) {
	t1, _ := newMockTransaction(t, true, -1)
	t1.Begin()

	b3, _ := NewBtree[int, string](ctx, "2phase2", 8, true, true, true, "", t1)
	b3.Add(ctx, 5000, "I am the value with 5000 key.")
	b3.Add(ctx, 5001, "I am the value with 5001 key.")

	twoPhase := t1.GetPhasedTransaction()

	if err := twoPhase.Phase1Commit(ctx); err == nil {
		// Call 3rd party DB integration, failure.
		if err2 := my3rdPartyDBlogic(true); err2 != nil {
			twoPhase.Rollback(ctx)

			t1, _ = newMockTransaction(t, true, -1)
			t1.Begin()
			twoPhase := t1.GetPhasedTransaction()

			// NewBtree is not part of the transaction, a trie created will not rollback.
			// Thus, OpenBtree call should succeed.
			b3, err := OpenBtree[int, string](ctx, "2phase2", t1)
			if err != nil {
				t.Errorf("OpenBtree('2phase2') failed, got %v, want nil.", err)
				t.FailNow()
			}

			b3.Add(ctx, 5000, "I am the value with 5000 key.")
			b3.Add(ctx, 5001, "I am the value with 5001 key.")

			if ok, _ := b3.FindOne(ctx, 5000, true); !ok || b3.GetCurrentKey() != 5000 {
				t.Errorf("FindOne(5000, true) failed, got = %v, want = 5000", b3.GetCurrentKey())
			}
			if ok, _ := b3.Next(ctx); !ok || b3.GetCurrentKey() != 5001 {
				t.Errorf("Next() failed, got = %v, want = 5001", b3.GetCurrentKey())
			}

			// Call 1st phase commit.
			if err = twoPhase.Phase1Commit(ctx); err != nil {
				t.Error(err)
			}

			// Call 3rd party DB integration, success.
			if err := my3rdPartyDBlogic(false); err != nil {
				t.Error(err)
			}
			// Call 2nd phase commit.
			if err = twoPhase.Phase2Commit(ctx); err != nil {
				t.Error(err)
			}
			return
		}
		t.Error("Should not go here.")
	} else {
		t.Error("Should not go here.")
	}
}

func my3rdPartyDBlogic(induceError bool) error {
	if induceError {
		return fmt.Errorf("Simulate error in 3rd party DB call/interaction.")
	}
	return nil
}
