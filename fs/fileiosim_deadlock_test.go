package fs

import (
	"context"
	"testing"
	"time"
)

// TestFileIOSimulator_WriteFilePanicDeadlock confirms that a panic in WriteFile
// (e.g., nil lookup map) does not leave sim.locker permanently held.
func TestFileIOSimulator_WriteFilePanicDeadlock(t *testing.T) {
	sim := newFileIOSim()
	sim.lookup = nil

	func() {
		defer func() {
			_ = recover()
		}()
		_ = sim.WriteFile(context.Background(), "test", []byte("data"), 0644)
	}()

	done := make(chan bool)
	go func() {
		sim.locker.Lock()
		sim.locker.Unlock()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("deadlock: sim.locker remained locked after panic in WriteFile")
	}
}
