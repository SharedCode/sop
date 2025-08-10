package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// Covers Phase1Commit preconditions and lightweight branches deterministically.
func Test_Transaction_Phase1Commit_Preconditions(t *testing.T) {
	ctx := context.Background()

	t.Run("errors_when_not_begun", func(t *testing.T) {
		// Use the helper to create a transaction in the initial state (phaseDone = -1)
		// so HasBegun() is false and Phase1Commit should error.
		tx, _ := newMockTwoPhaseCommitTransaction(t, sop.ForWriting, -1, true)
		if err := tx.Phase1Commit(ctx); err == nil {
			t.Fatalf("expected error when Phase1Commit called before Begin")
		}
	})

	t.Run("no_check_returns_nil", func(t *testing.T) {
		tr, _ := newMockTransaction(t, sop.NoCheck, -1)
		if err := tr.Begin(); err != nil {
			t.Fatalf("begin failed: %v", err)
		}
		if err := tr.GetPhasedTransaction().(*Transaction).Phase1Commit(ctx); err != nil {
			t.Fatalf("expected nil error in NoCheck mode, got %v", err)
		}
	})

	t.Run("reader_calls_conflict_check_only", func(t *testing.T) {
		tr, _ := newMockTransaction(t, sop.ForReading, -1)
		if err := tr.Begin(); err != nil {
			t.Fatalf("begin failed: %v", err)
		}
		if err := tr.GetPhasedTransaction().(*Transaction).Phase1Commit(ctx); err != nil {
			t.Fatalf("expected nil for reader Phase1Commit, got %v", err)
		}
	})
}

// Covers Phase2Commit preconditions and non-writer early-return path deterministically.
func Test_Transaction_Phase2Commit_Preconditions_ReaderAndNotBegun(t *testing.T) {
	ctx := context.Background()

	t.Run("errors_when_not_begun", func(t *testing.T) {
		tx, _ := newMockTwoPhaseCommitTransaction(t, sop.ForWriting, -1, true)
		if err := tx.Phase2Commit(ctx); err == nil {
			t.Fatalf("expected error when Phase2Commit called before Begin")
		}
	})

	t.Run("reader_returns_nil", func(t *testing.T) {
		tr, _ := newMockTransaction(t, sop.ForReading, -1)
		if err := tr.Begin(); err != nil {
			t.Fatalf("begin failed: %v", err)
		}
		// Simulate Phase1 done
		tr.GetPhasedTransaction().(*Transaction).phaseDone = 1
		if err := tr.GetPhasedTransaction().(*Transaction).Phase2Commit(ctx); err != nil {
			t.Fatalf("expected nil for reader Phase2Commit, got %v", err)
		}
	})
}

// Covers onIdle path: when an hour is being processed but no work exists, hourBeingProcessed resets.
func Test_Transaction_OnIdle_ResetsHourWhenNoWork(t *testing.T) {
	ctx := context.Background()

	// Save/restore globals to avoid leaking state across tests.
	prevHour := hourBeingProcessed
	prevLast := lastOnIdleRunTime
	prevNow := sop.Now
	defer func() {
		hourBeingProcessed = prevHour
		lastOnIdleRunTime = prevLast
		sop.Now = prevNow
	}()

	// Force scheduler window to run now.
	sop.Now = func() time.Time { return time.Now() }
	hourBeingProcessed = "2022010112"
	lastOnIdleRunTime = 0

	// Set up a lightweight transaction with a non-empty backend and a mock logger.
	tx := &Transaction{
		btreesBackend: []btreeBackend{{}},
		logger:        newTransactionLogger(mocks.NewMockTransactionLog(), true),
	}

	tx.onIdle(ctx)

	if hourBeingProcessed != "" {
		t.Fatalf("expected hourBeingProcessed reset to empty, got %q", hourBeingProcessed)
	}
	if lastOnIdleRunTime == 0 {
		t.Fatalf("expected lastOnIdleRunTime to be updated")
	}
}
