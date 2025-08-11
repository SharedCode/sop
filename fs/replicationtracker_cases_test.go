package fs

import (
    "context"
    "errors"
    "os"
    "path/filepath"
    "testing"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/common/mocks"
)

func TestReplicationTrackerFormatters(t *testing.T) {
    ctx := context.Background()
    base := t.TempDir()
    active := base + string(os.PathSeparator)
    passive := filepath.Join(base, "p")
    rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
    if err != nil { t.Fatalf("rt: %v", err) }

    id := sop.NewUUID()
    got := rt.formatActiveFolderEntity("x/y")
    if got != filepath.Join(active, "x/y") {
        t.Fatalf("formatActiveFolderEntity: %q", got)
    }
    rt.ActiveFolderToggler = false
    got = rt.formatPassiveFolderEntity(id.String())
    if got != filepath.Join(active, id.String()) { // when active toggled false, passive is first
        t.Fatalf("formatPassiveFolderEntity: %q", got)
    }
}

func TestReplicationTrackerWriteReadStatus(t *testing.T) {
    ctx := context.Background()
    active := filepath.Join(t.TempDir(), "a")
    passive := filepath.Join(t.TempDir(), "b")
    rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
    if err != nil { t.Fatalf("rt: %v", err) }
    // mutate and write
    rt.ActiveFolderToggler = true
    rt.FailedToReplicate = false
    if err := rt.writeReplicationStatus(ctx, rt.formatActiveFolderEntity(replicationStatusFilename)); err != nil {
        t.Fatalf("writeReplicationStatus: %v", err)
    }
    // read back
    rt2, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
    if err != nil { t.Fatalf("rt2: %v", err) }
    if err := rt2.readReplicationStatus(ctx, rt2.formatActiveFolderEntity(replicationStatusFilename)); err != nil {
        t.Fatalf("readReplicationStatus: %v", err)
    }
    if rt2.FailedToReplicate {
        t.Fatalf("FailedToReplicate should be false")
    }
}

func TestHandleReplicationRelatedErrorTriggersFailover(t *testing.T) {
    defer func() { GlobalReplicationDetails = nil }()
    ctx := context.Background()
    // Ensure no leftover global state interferes with this test's expectations.
    GlobalReplicationDetails = nil
    active := filepath.Join(t.TempDir(), "act")
    passive := filepath.Join(t.TempDir(), "pas")
    rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
    if err != nil { t.Fatalf("rt: %v", err) }
    rt.ActiveFolderToggler = true

    // trigger failover-qualified error
    ioErr := sop.Error{Code: sop.FailoverQualifiedError, Err: errors.New("io fail")}
    rt.HandleReplicationRelatedError(ctx, ioErr, nil, false)

    if rt.ActiveFolderToggler != false {
        t.Fatalf("expected toggler flipped to passive active")
    }
    if !rt.FailedToReplicate {
        t.Fatalf("expected FailedToReplicate true")
    }
    // status file should exist in (new) active (was passive before flipping)
    fn := rt.formatActiveFolderEntity(replicationStatusFilename)
    if _, err := os.Stat(fn); err != nil {
        t.Fatalf("expected replication status at %s: %v", fn, err)
    }
}

func TestSyncWithL2CachePushPull(t *testing.T) {
    defer func() { GlobalReplicationDetails = nil }()
    ctx := context.Background()
    active := filepath.Join(t.TempDir(), "a")
    passive := filepath.Join(t.TempDir(), "b")
    cache := mocks.NewMockClient()
    rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
    if err != nil { t.Fatalf("rt: %v", err) }
    rt.ActiveFolderToggler = true
    rt.FailedToReplicate = false
    GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: true}
    if err := rt.syncWithL2Cache(ctx, true); err != nil { t.Fatalf("push: %v", err) }
    // clear and pull
    GlobalReplicationDetails = nil
    if err := rt.syncWithL2Cache(ctx, false); err != nil { t.Fatalf("pull: %v", err) }
    if GlobalReplicationDetails == nil || !GlobalReplicationDetails.FailedToReplicate {
        t.Fatalf("expected pulled value set true")
    }
}

func TestLogCommitChangesWritesFile(t *testing.T) {
    ctx := context.Background()
    active := filepath.Join(t.TempDir(), "a")
    rt, err := NewReplicationTracker(ctx, []string{active, filepath.Join(t.TempDir(), "b")}, true, mocks.NewMockClient())
    if err != nil { t.Fatalf("rt: %v", err) }
    // enable logging of commit changes so the file gets written
    rt.LogCommitChanges = true
    // ensure active base exists to avoid any mkdir races
    if err := os.MkdirAll(active, 0o755); err != nil { t.Fatalf("mkdir active: %v", err) }
    tid := sop.NewUUID()
    if err := rt.logCommitChanges(ctx, tid, nil, nil, nil, nil, nil); err != nil {
        t.Fatalf("logCommitChanges: %v", err)
    }
    fn := rt.formatActiveFolderEntity(filepath.Join(commitChangesLogFolder, tid.String()+logFileExtension))
    if _, err := os.Stat(fn); err != nil { t.Fatalf("expected file: %v", err) }
}

func TestFileIOWithReplicationExists(t *testing.T) {
    ctx := context.Background()
    active := filepath.Join(t.TempDir(), "act")
    passive := filepath.Join(t.TempDir(), "pas")
    rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
    ms := NewManageStoreFolder(nil)
    fio := newFileIOWithReplication(rt, ms, false)
    // create a file under active and check exists
    name := "foo.bin"
    path := rt.formatActiveFolderEntity(name)
    if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil { t.Fatal(err) }
    if err := os.WriteFile(path, []byte("x"), 0o644); err != nil { t.Fatal(err) }
    if !fio.exists(ctx, name) { t.Fatalf("expected exists true") }
}
