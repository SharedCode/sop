package cache

import (
	"testing"
	"time"
)

// TestShardedMap_StorePanicDeadlock confirms that a panic in store (such as nil map assignment)
// leaves shard.mu permanently locked when Unlock is not deferred.
func TestShardedMap_StorePanicDeadlock(t *testing.T) {
	sm := newShardedMap(100)
	key := "test_key"
	shard := sm.getShard(key)
	shard.items = nil

	func() {
		defer func() {
			_ = recover()
		}()
		sm.store(key, "val")
	}()

	done := make(chan bool)
	go func() {
		shard.mu.Lock()
		shard.mu.Unlock()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("deadlock: shard.mu remained locked after panic in store")
	}
}

// TestShardedMap_LoadOrStorePanicDeadlock confirms that a panic in loadOrStore leaves
// shard.mu permanently locked when Unlock is not deferred.
func TestShardedMap_LoadOrStorePanicDeadlock(t *testing.T) {
	sm := newShardedMap(100)
	key := "test_key"
	shard := sm.getShard(key)
	shard.items = nil

	func() {
		defer func() {
			_ = recover()
		}()
		sm.loadOrStore(key, "val")
	}()

	done := make(chan bool)
	go func() {
		shard.mu.Lock()
		shard.mu.Unlock()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("deadlock: shard.mu remained locked after panic in loadOrStore")
	}
}
