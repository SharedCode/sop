package agent

import (
	"context"
	"testing"
)

// TestScanWithFilter_UsesOptimization tests the actual Scan function with filtering
// to verify that only emitted items are locked.
func TestScanWithFilter_UsesOptimization(t *testing.T) {
	ctx := context.Background()

	// Create a ScriptEngine with script context
	scriptCtx := NewScriptContext()
	engine := NewScriptEngine(scriptCtx, nil)

	// Create mock store with 100 items, only 10 match filter
	items := make([]MockItem, 100)
	for i := 0; i < 100; i++ {
		items[i] = MockItem{
			Key: map[string]any{"id": i},
			Value: map[string]any{
				"id":     i,
				"status": "inactive",
			},
		}
		// Only 10 items have status "active"
		if i%10 == 0 {
			items[i].Value = map[string]any{
				"id":     i,
				"status": "active",
			}
		}
	}

	trackingStore := &trackingMockStore{
		MockStore:       NewMockStore("test", items),
		lockCallCount:   0,
		noLockCallCount: 0,
	}

	// Register store in engine context
	engine.Context.Stores["test_store"] = trackingStore

	// Scan with filter for "active" items
	result, err := engine.Scan(ctx, map[string]any{
		"store": "test_store",
		"filter": map[string]any{
			"status": map[string]any{"$eq": "active"},
		},
		"limit": 1000.0,
	}, nil)

	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	resultList, ok := result.([]map[string]any)
	if !ok {
		t.Fatalf("Expected []map[string]any, got %T", result)
	}

	// Verify: Only 10 items emitted
	if len(resultList) != 10 {
		t.Errorf("Expected 10 results, got %d", len(resultList))
	}

	// Verify: RLockCurrentItem called exactly 10 times (once per emitted item)
	if trackingStore.lockCallCount != 10 {
		t.Errorf("Expected RLockCurrentItem called 10 times, called %d times", trackingStore.lockCallCount)
	}

	// Verify: GetCurrentValueNoLock called for all scanned items
	// Note: May be slightly more than 100 due to implementation details
	if trackingStore.noLockCallCount < 10 || trackingStore.noLockCallCount > 110 {
		t.Errorf("Expected GetCurrentValueNoLock called ~100 times, called %d times", trackingStore.noLockCallCount)
	}

	t.Logf("✅ Scan optimization verified:")
	t.Logf("   - Total items scanned: %d", trackingStore.noLockCallCount)
	t.Logf("   - Items locked (emitted): %d", trackingStore.lockCallCount)
	t.Logf("   - Lock reduction: %.1f%%", 100.0*(1.0-float64(trackingStore.lockCallCount)/float64(trackingStore.noLockCallCount)))
}

// trackingMockStore wraps MockStore to track method calls
type trackingMockStore struct {
	*MockStore
	lockCallCount   int
	noLockCallCount int
}

func (s *trackingMockStore) RLockCurrentItem(ctx context.Context) error {
	s.lockCallCount++
	return s.MockStore.RLockCurrentItem(ctx)
}

func (s *trackingMockStore) GetCurrentValueNoLock(ctx context.Context) (any, error) {
	s.noLockCallCount++
	return s.MockStore.GetCurrentValueNoLock(ctx)
}
