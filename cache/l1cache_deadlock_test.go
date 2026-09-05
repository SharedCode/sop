package cache

import (
	"testing"
	"time"
)

// TestGetGlobalL1Cache_NilPanicDeadlock confirms that calling GetGlobalL1Cache(nil)
// does not leave globalL1Locker permanently locked.
func TestGetGlobalL1Cache_NilPanicDeadlock(t *testing.T) {
	func() {
		defer func() {
			_ = recover()
		}()
		GetGlobalL1Cache(nil)
	}()

	done := make(chan bool)
	go func() {
		globalL1Locker.Lock()
		globalL1Locker.Unlock()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("deadlock: globalL1Locker remained locked after nil panic in GetGlobalL1Cache")
	}
}
