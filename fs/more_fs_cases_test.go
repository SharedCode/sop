package fs

import (
    "context"
    "os"
    "path/filepath"
    "testing"
    "time"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/common/mocks"
)

// Covers fileIO.read path through FileIOWithReplication wrapper.
func TestFileIOWithReplicationRead(t *testing.T) {
    ctx := context.Background()
    active := filepath.Join(t.TempDir(), "a")
    passive := filepath.Join(t.TempDir(), "p")
    rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
    ms := NewManageStoreFolder(nil)
    fio := newFileIOWithReplication(rt, ms, false)

    name := "foo.data"
    full := rt.formatActiveFolderEntity(name)
    if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil { t.Fatal(err) }
    if err := os.WriteFile(full, []byte("abc"), 0o644); err != nil { t.Fatal(err) }
    got, err := fio.read(ctx, name)
    if err != nil { t.Fatalf("read: %v", err) }
    if string(got) != "abc" { t.Fatalf("bad read: %q", string(got)) }
}

// Covers fileIOSimulator ToFilePath/Exists/ReadDir/MkdirAll/RemoveAll no-op behaviors.
func TestFileIOSimulatorBasics(t *testing.T) {
    sim := newFileIOSim()
    ctx := context.Background()
    if sim.ToFilePath("base", sop.NewUUID()) != "" { t.Fatalf("ToFilePath expected empty string") }
    if !sim.Exists(ctx, "any") { t.Fatalf("Exists expected true") }
    if err := sim.MkdirAll(ctx, "x", 0o755); err != nil { t.Fatalf("MkdirAll: %v", err) }
    if err := sim.RemoveAll(ctx, "x"); err != nil { t.Fatalf("RemoveAll: %v", err) }
    if _, err := sim.ReadDir(ctx, "x"); err != nil { t.Fatalf("ReadDir: %v", err) }
}

// Exercise replicationTracker.readStatusFromHomeFolder and handleFailedToReplicate branches.
func TestReplicationTrackerReadStatusAndFailureFlag(t *testing.T) {
    ctx := context.Background()
    base := t.TempDir()
    a := filepath.Join(base, "a")
    b := filepath.Join(base, "b")
    // Write status only in passive to force toggling when reading.
    rt, err := NewReplicationTracker(ctx, []string{a, b}, true, mocks.NewMockClient())
    if err != nil { t.Fatalf("rt: %v", err) }
    rt.ActiveFolderToggler = false // make b active initially for status write
    if err := os.MkdirAll(b, 0o755); err != nil { t.Fatal(err) }
    if err := rt.writeReplicationStatus(ctx, rt.formatActiveFolderEntity(replicationStatusFilename)); err != nil { t.Fatalf("write: %v", err) }

    // New tracker should flip to b when reading status from home folder
    rt2, err := NewReplicationTracker(ctx, []string{a, b}, true, mocks.NewMockClient())
    if err != nil { t.Fatalf("rt2: %v", err) }
    // Simulate a failure and ensure handleFailedToReplicate sets flags and writes status in active
    rt2.FailedToReplicate = false
    rt2.handleFailedToReplicate(ctx)
    if !rt2.FailedToReplicate { t.Fatalf("expected FailedToReplicate true") }
    fn := rt2.formatActiveFolderEntity(replicationStatusFilename)
    if _, err := os.Stat(fn); err != nil { t.Fatalf("status exists: %v", err) }
    // Age files to ensure coverage for date comparisons
    past := time.Now().Add(-10 * time.Minute)
    os.Chtimes(fn, past, past)
}
