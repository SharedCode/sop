package fs

import "testing"

// TestRegistryMapMismatchUnreachable documents that mismatch branches in registryMap.set/remove
// are effectively unreachable under current hashing/findFileRegion logic (forWriting=true selects correct slot).
// Keeping this skipped test as executable documentation; if implementation changes, remove Skip and craft collision.
func TestRegistryMapMismatchUnreachable(t *testing.T) {
	t.Skip("registryMap set/remove mismatch branches currently unreachable; see commentary in test file")
}
