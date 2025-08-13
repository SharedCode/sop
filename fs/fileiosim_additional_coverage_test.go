package fs

import (
	"context"
	"testing"
)

// Adds coverage for fileIOSimulator read not found path and resetFlag(false) branch.
func TestFileIOSim_ReadNotFoundAndResetFalse(t *testing.T) {
	sim := newFileIOSim()
	ctx := context.Background()
	// read missing file -> not found error
	if _, err := sim.ReadFile(ctx, "missing_99"); err == nil {
		t.Fatalf("expected not found error")
	}
	// set error flag, then disable reset so flags persist across induced errors
	sim.setErrorOnSuffixNumber2(5)
	sim.setResetFlag(false)
	if _, err := sim.ReadFile(ctx, "foo_5"); err == nil {
		t.Fatalf("expected induced error")
	}
	// second read should still error because resetFlag(false)
	if _, err := sim.ReadFile(ctx, "foo_5"); err == nil {
		t.Fatalf("expected induced error persist")
	}
}
