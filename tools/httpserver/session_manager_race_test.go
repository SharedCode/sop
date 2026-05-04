package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sharedcode/sop/ai"
)

func TestSessionManager_GetOrCreate_Race(t *testing.T) {
	sm := NewSessionManager(5)

	var wg sync.WaitGroup
	var countMu sync.Mutex
	var cloneCount int

	// Fire 100 requests concurrently for the exact same session_id
	numRequests := 100
	wg.Add(numRequests)
	for i := 0; i < numRequests; i++ {
		go func() {
			defer wg.Done()
			agent, mu := sm.GetOrCreate("race-session", func() ai.Agent[map[string]any] {
				countMu.Lock()
				cloneCount++
				countMu.Unlock()

				// Small delay to encourage race condition at builder stage
				time.Sleep(10 * time.Millisecond)

				return &mockAgent{id: "racer"}
			})

			if agent == nil || mu == nil {
				t.Error("Expected non-nil agent and mutex")
			}
		}()
	}

	wg.Wait()

	if cloneCount == 0 {
		t.Fatal("Expected at least 1 builder execution")
	}

	// Verify that ONLY ONE session instance was ultimately stored.
	if len(sm.lookup) != 1 {
		t.Fatalf("Expected exactly 1 session stored, got %d", len(sm.lookup))
	}
}

func TestSessionManager_Stress_MixedConcurrency(t *testing.T) {
	// Capacity of 50, but we will request 200 different sessions
	// This forces a high rate of evictions while concurrently building and fetching
	capacity := 50
	sm := NewSessionManager(capacity)

	var wg sync.WaitGroup
	numWorkers := 1000
	numDistinctSessions := 200

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()

			// Group workers so multiple workers hit the exact same session concurrently,
			// while overall hitting many different sessions to trigger evictions.
			sessionGrp := workerID % numDistinctSessions
			sessionID := fmt.Sprintf("stress-session-%d", sessionGrp)

			agent, mu := sm.GetOrCreate(sessionID, func() ai.Agent[map[string]any] {
				// Simulate slow agent builder to encourage overlaps
				time.Sleep(2 * time.Millisecond)
				return &mockAgent{id: sessionID}
			})

			if agent == nil || mu == nil {
				t.Error("Expected non-nil agent and mutex")
				return
			}

			// Test the per-session lock
			mu.Lock()
			// Simulate some LLM/Agent work time
			time.Sleep(1 * time.Millisecond)
			mu.Unlock()

			// Randomly close some sessions to test concurrent Close() vs GetOrCreate()
			if workerID%15 == 0 {
				sm.Close(sessionID)
			}
		}(i)
	}

	wg.Wait()

	sm.mu.Lock()
	defer sm.mu.Unlock()
	// Ensure we strictly enforce bounded capacity despite heavy concurrent puts/evictions
	if len(sm.lookup) > capacity {
		t.Fatalf("Capacity strictly exceeded: found %d items, max is %d", len(sm.lookup), capacity)
	}
}
