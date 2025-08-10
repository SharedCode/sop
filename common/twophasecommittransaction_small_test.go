package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
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
