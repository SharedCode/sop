package main

import (
	"testing"
)

func TestSessionManager_Eviction(t *testing.T) {
	sm := NewSessionManager(3)
	agent1 := &mockAgent{id: "1"}
	agent2 := &mockAgent{id: "2"}
	agent3 := &mockAgent{id: "3"}
	agent4 := &mockAgent{id: "4"}

	sm.Put("s1", agent1)
	sm.Put("s2", agent2)
	sm.Put("s3", agent3)

	// Since adding order was s1, s2, s3, the LRU tail is s1.
	// But let's Get("s1") to promote it to head (MRU).
	_, ok := sm.Get("s1")
	if !ok {
		t.Fatal("Expected s1 to be present")
	}

	// Now s2 is at the tail (LRU). Add s4. It should evict s2.
	sm.Put("s4", agent4)

	// Verify eviction
	if _, ok := sm.Get("s2"); ok {
		t.Fatal("Expected s2 to be evicted due to LRU policy")
	}

	// Verify others remain
	if _, ok := sm.Get("s1"); !ok {
		t.Fatal("Expected s1 to remain")
	}
	if _, ok := sm.Get("s3"); !ok {
		t.Fatal("Expected s3 to remain")
	}
	if _, ok := sm.Get("s4"); !ok {
		t.Fatal("Expected s4 to remain")
	}
}

func TestSessionManager_Close(t *testing.T) {
	sm := NewSessionManager(3)
	agent1 := &mockAgent{id: "1"}
	sm.Put("s1", agent1)

	if _, ok := sm.Get("s1"); !ok {
		t.Fatal("Expected s1 to be present")
	}

	sm.Close("s1")

	if _, ok := sm.Get("s1"); ok {
		t.Fatal("Expected s1 to be closed and missing")
	}
}

func TestSessionManager_PutExisting(t *testing.T) {
	sm := NewSessionManager(3)
	agent1 := &mockAgent{id: "1"}
	agent1b := &mockAgent{id: "1b"}

	sm.Put("s1", agent1)
	sm.Put("s1", agent1b)

	a, ok := sm.Get("s1")
	if !ok {
		t.Fatal("Expected s1 to be present")
	}
	if a.(*mockAgent).id != "1b" {
		t.Fatal("Expected updated agent")
	}
}
