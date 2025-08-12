package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
	cas "github.com/sharedcode/sop/internal/cassandra"
)

func Test_ProcessExpiredTransactionLogs_ConsumesHourAndClears(t *testing.T) {
	ctx := context.Background()
	// Freeze time to seed logs into an old hour bucket
	origNow := cas.Now
	base := time.Date(2025, 1, 2, 15, 0, 0, 0, time.UTC)
	cas.Now = func() time.Time { return base }
	defer func() { cas.Now = origNow }()

	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	// Seed a minimal log entry via tl.log; finalizeCommit with nil is enough to make rollback remove logs
	if err := tl.log(ctx, finalizeCommit, nil); err != nil {
		t.Fatalf("seed log error: %v", err)
	}

	// Advance time beyond an hour so GetOne returns our TID for processing
	cas.Now = func() time.Time { return base.Add(2 * time.Hour) }

	tx := &Transaction{}
	hourBeingProcessed = "" // ensure GetOne path is taken
	if err := tl.processExpiredTransactionLogs(ctx, tx); err != nil {
		t.Fatalf("processExpiredTransactionLogs returned error: %v", err)
	}

	// Next call should hit GetOneOfHour and clear the hour since nothing else remains in that bucket
	if err := tl.processExpiredTransactionLogs(ctx, tx); err != nil {
		t.Fatalf("second processExpiredTransactionLogs error: %v", err)
	}
	if hourBeingProcessed != "" {
		t.Fatalf("expected hourBeingProcessed cleared; got %q", hourBeingProcessed)
	}
}

func Test_PriorityRollback_NilTransaction_NoPanic(t *testing.T) {
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	if err := tl.priorityRollback(context.Background(), nil, sop.NewUUID()); err != nil {
		t.Fatalf("priorityRollback with nil transaction returned error: %v", err)
	}
}
