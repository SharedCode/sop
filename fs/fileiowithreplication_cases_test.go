package fs

import (
    "bytes"
    "context"
    "path/filepath"
    "testing"
)

func TestFileIOWithReplicationActionsAndReplay(t *testing.T) {
    ctx := context.Background()
    active := filepath.Join(t.TempDir(), "active")
    passive := filepath.Join(t.TempDir(), "passive")

    // replication tracker with replicate initially disabled, then enabled for replay
    rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, nil)
    if err != nil {
        t.Fatalf("tracker: %v", err)
    }
    // ensure we start with first folder active
    rt.ActiveFolderToggler = true

    ms := NewManageStoreFolder(nil)
    fio := newFileIOWithReplication(rt, ms, true)

    // create store and write a file on active
    if err := fio.createStore(ctx, "storeA"); err != nil {
        t.Fatalf("createStore: %v", err)
    }
    name := filepath.Join("storeA", "file.bin")
    payload := []byte("payload")
    if err := fio.write(ctx, name, payload); err != nil {
        t.Fatalf("write: %v", err)
    }

    // not yet replicated; enable replicate and replay
    rt.replicate = true
    if err := fio.replicate(ctx); err != nil {
        t.Fatalf("replicate: %v", err)
    }

    // verify passive has the file with same content
    passivePath := rt.formatPassiveFolderEntity(name)
    got, err := fio.fio.ReadFile(ctx, passivePath)
    if err != nil {
        t.Fatalf("read passive: %v", err)
    }
    if !bytes.Equal(got, payload) {
        t.Fatalf("mismatch: got %q want %q", string(got), string(payload))
    }

    // remove store and replicate removal
    if err := fio.removeStore(ctx, "storeA"); err != nil {
        t.Fatalf("removeStore: %v", err)
    }
    if err := fio.replicate(ctx); err != nil {
        t.Fatalf("replicate remove: %v", err)
    }
}
