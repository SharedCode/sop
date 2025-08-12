package fs

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestStoreRepositoryUpdateUndoOnSecondFailure triggers a mid-update write failure on the second store
// and verifies the undo routine restores the first store's metadata (Count + Timestamp).
func TestStoreRepositoryUpdateUndoOnSecondFailure(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
	sr, err := NewStoreRepository(ctx, rt, nil, l2, 0)
	if err != nil {
		t.Fatalf("NewStoreRepository: %v", err)
	}

	s1 := sop.NewStoreInfo(sop.StoreOptions{Name: "aaa", SlotLength: 10})
	s2 := sop.NewStoreInfo(sop.StoreOptions{Name: "bbb", SlotLength: 10})
	s1.Timestamp = 111
	s2.Timestamp = 222
	if err := sr.Add(ctx, *s1, *s2); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Prepare updates; create directory to force write failure for s2.
	upd1 := *s1
	upd1.CountDelta = 3
	upd1.Timestamp = 999
	upd1.CacheConfig.StoreInfoCacheDuration = time.Minute
	upd2 := *s2
	upd2.CountDelta = 5
	upd2.Timestamp = 888
	upd2.CacheConfig.StoreInfoCacheDuration = time.Minute
	s2File := filepath.Join(base, upd2.Name, "storeinfo.txt")
	if err := os.Remove(s2File); err != nil {
		t.Fatalf("remove s2 storeinfo: %v", err)
	}
	if err := os.Mkdir(s2File, 0o755); err != nil {
		t.Fatalf("mkdir simulate failure: %v", err)
	}

	if _, err := sr.Update(ctx, []sop.StoreInfo{upd1, upd2}); err == nil {
		t.Fatalf("expected Update error due to second store write failure")
	}

	// Read back s1 file directly to assert rollback (CountDelta unapplied, timestamp restored)
	s1File := filepath.Join(base, s1.Name, "storeinfo.txt")
	ba, err := os.ReadFile(s1File)
	if err != nil {
		t.Fatalf("read s1 file: %v", err)
	}
	var got1 sop.StoreInfo
	if err := json.Unmarshal(ba, &got1); err != nil {
		t.Fatalf("unmarshal s1: %v", err)
	}
	if got1.Count != 0 {
		t.Fatalf("expected rollback Count=0, got %d", got1.Count)
	}
	if got1.Timestamp != 111 {
		t.Fatalf("expected rollback Timestamp=111, got %d", got1.Timestamp)
	}
}
