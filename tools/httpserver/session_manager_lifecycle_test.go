package main

import (
	"testing"
	"time"
)

func TestSessionManager_ComprehensiveLifecycle(t *testing.T) {
	sm := NewSessionManager(3)

	// 1. Starting Session
	sm.Put("s1", &mockAgent{id: "s1"})
	time.Sleep(10 * time.Millisecond)

	sm.Put("s2", &mockAgent{id: "s2"})
	time.Sleep(10 * time.Millisecond)

	sm.Put("s3", &mockAgent{id: "s3"})

	// Ensure they exist
	if _, ok := sm.Get("s1"); !ok {
		t.Fatal("s1 should exist")
	}

	time.Sleep(10 * time.Millisecond)

	// 2. Max Limit Overflow (s2 is currently tail since s1 was just accessed)
	sm.Put("s4", &mockAgent{id: "s4"})

	if _, ok := sm.Get("s2"); ok {
		t.Fatal("s2 should have been evicted due to bounded max-limit capacity")
	}

	// Wait deliberately
	time.Sleep(30 * time.Millisecond)

	// Refresh s4 so it's fresh (s1 and s3 become stale)
	sm.Get("s4")

	// 3. Stale-ing
	// After 30ms sleep, s1 and s3 are >= 30ms old. s4 is 0ms old.
	// Give a 15ms TTL window.
	evictedCount := sm.RemoveStale(15 * time.Millisecond)

	if evictedCount != 2 {
		t.Fatalf("Expected 2 stale sessions (s1, s3) to be evicted, got %d", evictedCount)
	}

	if _, ok := sm.Get("s1"); ok {
		t.Fatal("s1 should have been staled out and evicted")
	}
	if _, ok := sm.Get("s3"); ok {
		t.Fatal("s3 should have been staled out and evicted")
	}
	if _, ok := sm.Get("s4"); !ok {
		t.Fatal("s4 should have persisted past stale out because we refreshed it")
	}

	// 4. Closing explicitly
	sm.Close("s4")

	if _, ok := sm.Get("s4"); ok {
		t.Fatal("s4 should have been explicitly closed")
	}

	if len(sm.lookup) != 0 {
		t.Fatalf("Expected strictly 0 sessions left, got %d", len(sm.lookup))
	}
}
