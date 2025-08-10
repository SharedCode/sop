package common

import (
	"testing"

	"github.com/sharedcode/sop"
)

func Test_NewTwoPhaseCommitTransaction_Defaults(t *testing.T) {
	trans, err := newMockTwoPhaseCommitTransaction(t, sop.ForWriting, 0, false)
	if err != nil {
		t.Fatalf("newMockTwoPhaseCommitTransaction error: %v", err)
	}
	if trans.GetMode() != sop.ForWriting {
		t.Fatalf("mode mismatch: %v", trans.GetMode())
	}
	if !trans.HasBegun() {
		_ = trans.Begin()
	}
	if err := trans.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
}

func Test_ReaderTransaction_CommitChecksOnly(t *testing.T) {
	trans, _ := newMockTransaction(t, sop.ForReading, -1)
	if err := trans.Begin(); err != nil {
		t.Fatalf("Begin error: %v", err)
	}
	// Commit for reader should be a no-op/errorless
	if err := trans.Commit(ctx); err != nil {
		t.Fatalf("reader Commit error: %v", err)
	}
}
