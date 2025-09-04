package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop/common/mocks"
)

// infoRunCache wraps a sop.Cache and lets tests mutate the runID returned by Info.
// TestCacheRestartHelper_Throttling validates that repeated calls within the interval return false and do not error.
func TestCacheRestartHelper_Throttling(t *testing.T) {
	ctx := context.Background()
	base := mocks.NewMockClient()
	SetRestartCheckInterval(50 * time.Millisecond)
	SetRestartInfoEveryN(1)

	h := newCacheRestartHelper(base)
	// First call: no prior run_id so should return false (nothing to compare yet)
	if restarted, err := h.IsRestarted(ctx); err != nil || restarted {
		t.Fatalf("expected first call false,nil got %v,%v", restarted, err)
	}
	// Immediate second call within interval should be throttled -> false
	if restarted, err := h.IsRestarted(ctx); err != nil || restarted {
		t.Fatalf("expected throttled call false,nil got %v,%v", restarted, err)
	}
	// After interval elapses, call again -> still false with static run_id.
	time.Sleep(60 * time.Millisecond)
	if restarted, err := h.IsRestarted(ctx); err != nil || restarted {
		t.Fatalf("expected post-interval false,nil got %v,%v", restarted, err)
	}
}
