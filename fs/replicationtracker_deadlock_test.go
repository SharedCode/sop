package fs

import (
	"context"
	"testing"
	"time"
)

// TestReplicationTracker_HandleFailedToReplicatePanicDeadlock confirms that a panic
// inside handleFailedToReplicate (such as when GlobalReplicationDetails is nil)
// leaves globalReplicationDetailsLocker permanently held when Unlock is not deferred.
func TestReplicationTracker_HandleFailedToReplicatePanicDeadlock(t *testing.T) {
	globalReplicationDetailsLocker.Lock()
	saved := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	globalReplicationDetailsLocker.Unlock()
	defer func() {
		globalReplicationDetailsLocker.Lock()
		GlobalReplicationDetails = saved
		globalReplicationDetailsLocker.Unlock()
	}()

	rt := &replicationTracker{
		replicate: true,
	}

	func() {
		defer func() {
			_ = recover()
		}()
		rt.handleFailedToReplicate(context.Background())
	}()

	done := make(chan bool)
	go func() {
		globalReplicationDetailsLocker.Lock()
		globalReplicationDetailsLocker.Unlock()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("deadlock: globalReplicationDetailsLocker remained locked after panic in handleFailedToReplicate")
	}
}
