package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

// TestReplicationTracker_fastForward_Basic creates a commit log file and ensures fastForward consumes it.
func TestReplicationTracker_fastForward_Basic(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	act := filepath.Join(base, "act")
	pas := filepath.Join(base, "pas")
	if err := os.MkdirAll(filepath.Join(act, commitChangesLogFolder), 0o755); err != nil {
		t.Fatalf("mkdir act commitlogs: %v", err)
	}
	if err := os.MkdirAll(pas, 0o755); err != nil {
		t.Fatalf("mkdir pas: %v", err)
	}

	GlobalReplicationDetails = nil

	// Seed replication status so tracker starts with active toggler true.
	if err := os.WriteFile(filepath.Join(act, replicationStatusFilename), []byte(`{"FailedToReplicate":true,"ActiveFolderToggler":true,"LogCommitChanges":false}`), 0o644); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	rt, err := NewReplicationTracker(ctx, []string{act, pas}, true, nil)
	if err != nil {
		t.Fatalf("new tracker: %v", err)
	}

	// Prepare a simple store repository state so fastForward can instantiate components.
	store := sop.NewStoreInfo(sop.StoreOptions{Name: "c1", SlotLength: 4})
	// Write initial store list & store info into active side so replication logic sees them when applying log.
	if err := os.MkdirAll(filepath.Join(act, store.Name), 0o755); err != nil {
		t.Fatalf("mkdir store: %v", err)
	}
	if err := os.WriteFile(filepath.Join(act, store.Name, storeInfoFilename), []byte("{}"), 0o644); err != nil {
		t.Fatalf("seed storeinfo: %v", err)
	}
	if err := os.WriteFile(filepath.Join(act, storeListFilename), []byte(`["c1"]`), 0o644); err != nil {
		t.Fatalf("seed storelist: %v", err)
	}
	// And a dummy registry hash mod value (used by fastForward path).
	if err := os.WriteFile(filepath.Join(act, registryHashModValueFilename), []byte("4"), 0o644); err != nil {
		t.Fatalf("seed reghashmod: %v", err)
	}

	// Create minimal commit log payload with empty slices (no store replication changes, just exercise loop & delete).
	payload := []byte(`{"First":null,"Second":[[],[],[],[]]}`)
	logFile := filepath.Join(act, commitChangesLogFolder, time.Now().Format("20060102150405")+logFileExtension)
	if err := os.WriteFile(logFile, payload, 0o644); err != nil {
		t.Fatalf("write commit log: %v", err)
	}

	found, err := rt.fastForward(ctx)
	if err != nil {
		t.Fatalf("fastForward error: %v", err)
	}
	if !found {
		t.Fatalf("expected commit log to be processed")
	}
	if _, err := os.Stat(logFile); !os.IsNotExist(err) {
		t.Fatalf("expected commit log file to be deleted after processing")
	}
}

// TestReplicationTracker_fastForward exercises fastForward in two modes:
// 1. Empty operations payload (basic consumption & deletion)
// 2. Non-empty payload with new/add/update handles (enriched scenario)
func TestReplicationTracker_fastForward(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	act := filepath.Join(base, "act")
	pas := filepath.Join(base, "pas")
	if err := os.MkdirAll(filepath.Join(act, commitChangesLogFolder), 0o755); err != nil {
		t.Fatalf("mkdir act commitlogs: %v", err)
	}
	if err := os.MkdirAll(pas, 0o755); err != nil {
		t.Fatalf("mkdir pas: %v", err)
	}

	GlobalReplicationDetails = nil
	// Seed active replication status file.
	if err := os.WriteFile(filepath.Join(act, replicationStatusFilename), []byte(`{"FailedToReplicate":true,"ActiveFolderToggler":true,"LogCommitChanges":false}`), 0o644); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	cache := mocks.NewMockClient()
	rt, err := NewReplicationTracker(ctx, []string{act, pas}, true, cache)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}
	// Simulate failed replicate so fastForward flips flag to false internally before Replicate calls.
	rt.FailedToReplicate = true

	// Seed store list & info + registry hash mod.
	store := sop.NewStoreInfo(sop.StoreOptions{Name: "s1", SlotLength: 4})
	store.Count = 5
	if err := os.MkdirAll(filepath.Join(act, store.Name), 0o755); err != nil {
		t.Fatalf("mkdir store: %v", err)
	}
	if err := os.WriteFile(filepath.Join(act, store.Name, storeInfoFilename), []byte("{}"), 0o644); err != nil {
		t.Fatalf("seed storeinfo: %v", err)
	}
	if err := os.WriteFile(filepath.Join(act, storeListFilename), []byte(`["s1"]`), 0o644); err != nil {
		t.Fatalf("seed storelist: %v", err)
	}
	if err := os.WriteFile(filepath.Join(act, registryHashModValueFilename), []byte("4"), 0o644); err != nil {
		t.Fatalf("seed reghashmod: %v", err)
	}

	// Pre-populate cache file for GetRegistryHashModValue to read (already done) and create a minimal registry segment in active
	// so registry.Replicate path can attempt copies later (segment copying happens in copyStores, not here, but registry operations rely on hash mod).

	// Scenario 1: empty payload (basic)
	basicPayload := []byte(`{"First":null,"Second":[[],[],[],[]]}`)
	basicLog := filepath.Join(act, commitChangesLogFolder, time.Now().Add(1*time.Second).Format("20060102150405")+logFileExtension)
	if err := os.WriteFile(basicLog, basicPayload, 0o644); err != nil {
		t.Fatalf("write basic log: %v", err)
	}

	// Prepare handles for enriched scenario.
	newRoot := sop.NewHandle(sop.NewUUID())
	added := sop.NewHandle(sop.NewUUID())
	updated := sop.NewHandle(sop.NewUUID())
	updated.Version = 2
	// Skip removal handle to avoid triggering remove error path (already covered elsewhere).

	payload := sop.Tuple[[]sop.StoreInfo, [][]sop.RegistryPayload[sop.Handle]]{First: []sop.StoreInfo{*store}, Second: [][]sop.RegistryPayload[sop.Handle]{
		{{RegistryTable: "s1_r", IDs: []sop.Handle{newRoot}}},
		{{RegistryTable: "s1_r", IDs: []sop.Handle{added}}},
		{{RegistryTable: "s1_r", IDs: []sop.Handle{updated}}},
		{},
	}}
	ba, _ := encoding.DefaultMarshaler.Marshal(payload)
	enrichedLog := filepath.Join(act, commitChangesLogFolder, time.Now().Add(2*time.Second).Format("20060102150405")+logFileExtension)
	if err := os.WriteFile(enrichedLog, ba, 0o644); err != nil {
		t.Fatalf("write enriched log: %v", err)
	}

	// First fastForward should process the newest file first (descending sort) – enriched – then basic on second call.
	// We loop until no files remain, asserting both get deleted.
	processed := map[string]bool{}
	for {
		found, err := rt.fastForward(ctx)
		if err != nil {
			t.Fatalf("fastForward: %v", err)
		}
		if !found {
			break
		}
		// Track deletions.
		if _, err := os.Stat(enrichedLog); os.IsNotExist(err) {
			processed[enrichedLog] = true
		}
		if _, err := os.Stat(basicLog); os.IsNotExist(err) {
			processed[basicLog] = true
		}
	}
	if !processed[enrichedLog] || !processed[basicLog] {
		t.Fatalf("expected both logs processed, got %+v", processed)
	}
	if rt.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate cleared for replication operations")
	}
}
