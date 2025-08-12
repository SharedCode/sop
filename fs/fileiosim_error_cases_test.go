package fs

import (
	"context"
	"testing"
)

// Exercises fileIOSimulator error injection & reset logic (setErrorOnSuffixNumber, setErrorOnSuffixNumber2, setResetFlag).
func TestFileIOSimulatorErrorInjectionAndReset(t *testing.T) {
	sim := newFileIOSim()
	ctx := context.Background()

	// Cause WriteFile error for suffix _1
	sim.setErrorOnSuffixNumber(1)
	if err := sim.WriteFile(ctx, "file_1", []byte("x"), 0o644); err == nil {
		t.Fatalf("expected induced write error on suffix _1")
	}
	// Write succeeds on different suffix
	if err := sim.WriteFile(ctx, "file_2", []byte("ok"), 0o644); err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	// Read succeeds initially
	if _, err := sim.ReadFile(ctx, "file_2"); err != nil {
		t.Fatalf("read ok: %v", err)
	}

	// Induce read error on suffix _2 that also resets errors afterward.
	sim.setErrorOnSuffixNumber2(2)
	sim.setResetFlag(true)
	if _, err := sim.ReadFile(ctx, "file_2"); err == nil {
		t.Fatalf("expected induced read error on suffix _2")
	}
	// Next read should succeed because resetFlag triggered clearing.
	if _, err := sim.ReadFile(ctx, "file_2"); err != nil {
		t.Fatalf("expected read success after reset: %v", err)
	}

	// Remove honors write error flag (set again) then normal remove.
	sim.setErrorOnSuffixNumber(2)
	if err := sim.Remove(ctx, "file_2"); err == nil {
		t.Fatalf("expected induced remove error on suffix _2")
	}
	sim.setErrorOnSuffixNumber(-1)
	if err := sim.Remove(ctx, "file_2"); err != nil {
		t.Fatalf("remove: %v", err)
	}
}
