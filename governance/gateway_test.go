package governance

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestGatewayAuthenticatorAndRateLimiter(t *testing.T) {
	ctx := context.Background()

	auth := NewStaticTokenAuthenticator()
	identity := &GatewayIdentity{
		CallerID:    "agent-007",
		TenantID:    "corp-alpha",
		WorkspaceID: "ws-finance",
		Tier:        TierPro,
		Roles:       []string{"Analyst"},
	}
	auth.RegisterToken("secret-token-123", identity)

	// Valid auth
	id, err := auth.Authenticate(ctx, "secret-token-123")
	if err != nil {
		t.Fatalf("failed to authenticate valid token: %v", err)
	}
	if id.CallerID != "agent-007" || id.Tier != TierPro {
		t.Errorf("mismatched identity: %+v", id)
	}

	// Invalid auth
	_, err = auth.Authenticate(ctx, "bad-token")
	if err == nil {
		t.Fatalf("expected error on bad token, got nil")
	}

	// Priority calculation
	if PriorityWeight(TierCore) >= PriorityWeight(TierPro) {
		t.Errorf("Pro priority should exceed Core priority")
	}
	if PriorityWeight(TierPro) >= PriorityWeight(TierEnterprise) {
		t.Errorf("Enterprise priority should exceed Pro priority")
	}

	// Rate limiter
	limiter := NewTierRateLimiter()
	allowed, _, err := limiter.Allow(ctx, identity)
	if err != nil || !allowed {
		t.Fatalf("expected first request to be allowed, got allowed=%v err=%v", allowed, err)
	}
}

func TestTierRateLimiter_MemoryStaysBoundedWhenKeysKeepChanging(t *testing.T) {
	rl := NewTierRateLimiter()
	for i := 0; i < maxBuckets+2000; i++ {
		rl.Allow(context.Background(), &GatewayIdentity{TenantID: fmt.Sprintf("client-%d", i), Tier: TierCore})
	}
	if n := len(rl.buckets); n > maxBuckets+1 {
		t.Errorf("limiter holds %d buckets, want at most %d", n, maxBuckets+1)
	}
}

func TestTierRateLimiter_DropsIdleBucketsWhenFull(t *testing.T) {
	rl := NewTierRateLimiter()
	for i := 0; i < maxBuckets; i++ {
		rl.Allow(context.Background(), &GatewayIdentity{TenantID: fmt.Sprintf("client-%d", i), Tier: TierCore})
	}
	old := time.Now().Add(-2 * bucketIdleTTL)
	for _, b := range rl.buckets {
		b.lastRefill = old
	}
	rl.lastSweep = time.Time{}

	rl.Allow(context.Background(), &GatewayIdentity{TenantID: "new-client", Tier: TierCore})
	if n := len(rl.buckets); n > 2 {
		t.Errorf("idle buckets should have been dropped, %d remain", n)
	}
}

func TestTierRateLimiter_StillLimitsAKeyAfterTheSweepLogicRuns(t *testing.T) {
	rl := NewTierRateLimiter()
	id := &GatewayIdentity{TenantID: "one-client", Tier: TierCore}
	allowed := 0
	for i := 0; i < 100; i++ {
		if ok, _, _ := rl.Allow(context.Background(), id); ok {
			allowed++
		}
	}
	if allowed < 15 || allowed > 40 {
		t.Errorf("a free-tier burst should be limited near its capacity of 20, got %d of 100", allowed)
	}
}
