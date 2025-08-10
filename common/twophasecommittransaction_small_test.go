package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/common/mocks"
)

// Covers constructor branches (defaulting and max cap) and simple accessors.
func Test_NewTwoPhaseCommitTransaction_And_Accessors(t *testing.T) {
	// default maxTime path (<=0)
	tr, err := NewTwoPhaseCommitTransaction(sop.ForReading, 0, false, mockNodeBlobStore, mockStoreRepository, mockRegistry, mockRedisCache, mocks.NewMockTransactionLog())
	if err != nil {
		t.Fatalf("ctor error: %v", err)
	}
	if tr == nil {
		t.Fatalf("expected non-nil transaction")
	}

	// verify Begin/HasBegun and Close paths minimally
	if err := tr.Begin(); err != nil {
		t.Fatalf("begin err: %v", err)
	}
	if !tr.HasBegun() {
		t.Fatalf("HasBegun should be true after Begin")
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("close err: %v", err)
	}

	// accessors
	if tr.GetStoreRepository() == nil {
		t.Fatalf("GetStoreRepository returned nil")
	}
	// Seed some stores and verify GetStores forwards
	_ = mockStoreRepository.Add(context.Background(), *sop.NewStoreInfo(sop.StoreOptions{Name: "s1", SlotLength: 8}))
	_ = mockStoreRepository.Add(context.Background(), *sop.NewStoreInfo(sop.StoreOptions{Name: "s2", SlotLength: 8}))
	names, err := tr.GetStores(context.Background())
	if err != nil || len(names) < 2 {
		t.Fatalf("GetStores got=%v err=%v", names, err)
	}

	// max cap path (>1h gets capped)
	tr2, err := NewTwoPhaseCommitTransaction(sop.ForReading, 3*time.Hour, false, mockNodeBlobStore, mockStoreRepository, mockRegistry, mockRedisCache, mocks.NewMockTransactionLog())
	if err != nil || tr2 == nil {
		t.Fatalf("ctor(>1h) err=%v tr2=%v", err, tr2)
	}
}

func Test_Transaction_Begin_Errors(t *testing.T) {
	tr := &Transaction{phaseDone: -1}
	if err := tr.Begin(); err != nil {
		t.Fatalf("first begin should succeed: %v", err)
	}
	if err := tr.Begin(); err == nil {
		t.Fatalf("second begin should fail")
	}

	tr2 := &Transaction{phaseDone: 2}
	if err := tr2.Begin(); err == nil {
		t.Fatalf("begin after done should fail")
	}
}

func Test_Transaction_Phase2Commit_Preconditions(t *testing.T) {
	ctx := context.Background()
	tr := &Transaction{phaseDone: -1}
	if err := tr.Phase2Commit(ctx); err == nil {
		t.Fatalf("Phase2Commit should fail when not begun")
	}

	tr2 := &Transaction{phaseDone: -1}
	if err := tr2.Begin(); err != nil {
		t.Fatalf("begin err: %v", err)
	}
	// phaseDone==0 after Begin
	if err := tr2.Phase2Commit(ctx); err == nil {
		t.Fatalf("Phase2Commit should error when phase 1 not invoked")
	}
}

func Test_Transaction_Rollback_Preconditions(t *testing.T) {
	ctx := context.Background()
	tr := &Transaction{phaseDone: -1}
	if err := tr.Rollback(ctx, nil); err == nil {
		t.Fatalf("Rollback should fail when not begun")
	}
}

func Test_DeleteObsoleteEntries_Smoke(t *testing.T) {
	ctx := context.Background()
	tr := &Transaction{blobStore: mockNodeBlobStore, registry: mockRegistry, l1Cache: cache.GetGlobalCache()}
	// One deleted registry ID and one unused node blob
	del := []sop.RegistryPayload[sop.UUID]{{IDs: []sop.UUID{sop.NewUUID()}}}
	unused := []sop.BlobsPayload[sop.UUID]{{Blobs: []sop.UUID{sop.NewUUID()}}}
	if err := tr.deleteObsoleteEntries(ctx, del, unused); err != nil {
		t.Fatalf("deleteObsoleteEntries err: %v", err)
	}
}
