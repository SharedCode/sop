package fs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// mockCacheSetStructWarn forces SetStruct warning paths during Update.
type mockCacheSetStructWarn struct{ sop.Cache }

func (m mockCacheSetStructWarn) SetStruct(ctx context.Context, key string, value interface{}, d time.Duration) error {
	return fmt.Errorf("fail setstruct")
}

// TestStoreRepositoryUpdateCacheWarning covers warning branch when cache.SetStruct fails in Update.
func TestStoreRepositoryUpdateCacheWarning(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	// Wrap mock to fail SetStruct but allow other ops.
	failingCache := mockCacheSetStructWarn{Cache: mocks.NewMockClient()}
	sr, _ := NewStoreRepository(ctx, rt, nil, failingCache, 0)
	s := *sop.NewStoreInfo(sop.StoreOptions{Name: "cw", SlotLength: 10})
	if err := sr.Add(ctx, s); err != nil {
		t.Fatalf("Add: %v", err)
	}
	s.CountDelta = 2
	s.CacheConfig.StoreInfoCacheDuration = time.Second
	if _, err := sr.Update(ctx, []sop.StoreInfo{s}); err != nil {
		t.Fatalf("Update should succeed despite cache warning: %v", err)
	}
}
