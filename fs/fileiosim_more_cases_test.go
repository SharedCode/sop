package fs

import (
    "context"
    "testing"
)

// Covers fileIOSimulator.setResetFlag by inducing an error on suffix 0 that resets flags,
// then verifying a subsequent read succeeds.
func TestFileIOSim_ResetFlagClearsErrors(t *testing.T) {
    sim := newFileIOSim()
    ctx := context.Background()

    // Seed content for a filename with suffix _0
    name := "dummy_0"
    if err := sim.WriteFile(ctx, name, []byte("ok"), 0o644); err != nil {
        t.Fatalf("seed write: %v", err)
    }

    // Configure simulator to error on suffix 0 via the second flag and enable auto-reset.
    sim.setResetFlag(true)
    sim.setErrorOnSuffixNumber2(0)

    // First read should error and reset the flags internally.
    if _, err := sim.ReadFile(ctx, name); err == nil {
        t.Fatalf("expected induced read error, got nil")
    }

    // Next read should succeed because flags were reset.
    if b, err := sim.ReadFile(ctx, name); err != nil || string(b) != "ok" {
        t.Fatalf("read after reset failed: %v, %q", err, string(b))
    }
}
