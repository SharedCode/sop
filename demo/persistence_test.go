//go:build js && wasm

package main

import (
	"testing"

	"syscall/js"
)

// opfsBridgeJS is the exact same JS source defined in index.html's inline
// <script>. This test evals it into the blank page wasmbrowsertest runs Go
// tests in, so the test exercises the identical code path production uses
// instead of a reimplementation of it.
const opfsBridgeJS = `
function opfsSupported() {
  return !!(navigator.storage && navigator.storage.getDirectory);
}
async function opfsWriteFile(path, bytes) {
  const root = await navigator.storage.getDirectory();
  const fileHandle = await root.getFileHandle(path, { create: true });
  const writable = await fileHandle.createWritable();
  await writable.write(bytes);
  await writable.close();
  return true;
}
async function opfsReadFile(path) {
  const root = await navigator.storage.getDirectory();
  const fileHandle = await root.getFileHandle(path, { create: false });
  const file = await fileHandle.getFile();
  const buf = await file.arrayBuffer();
  return new Uint8Array(buf);
}
async function opfsRemoveFile(path) {
  const root = await navigator.storage.getDirectory();
  await root.removeEntry(path);
  return true;
}
`

func requireOPFSBridge(t *testing.T) {
	t.Helper()
	js.Global().Call("eval", opfsBridgeJS)
	if !opfsAvailable() {
		t.Skip("OPFS not available in this browser/test environment")
	}
}

// Test_OPFS_WriteReadRoundTrip verifies the real async File System Access
// API round trip: write bytes, read them back, byte-for-byte.
func Test_OPFS_WriteReadRoundTrip(t *testing.T) {
	requireOPFSBridge(t)
	const name = "test-roundtrip.bin"
	defer opfsRemoveFile(name)

	want := []byte(`{"hello":"opfs","n":42}`)
	if err := opfsWriteFile(name, want); err != nil {
		t.Fatalf("opfsWriteFile: %v", err)
	}
	got, err := opfsReadFile(name)
	if err != nil {
		t.Fatalf("opfsReadFile: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("round trip mismatch: want %q, got %q", want, got)
	}
}

// Test_OPFS_ReadMissing_Errors verifies reading a file that was never
// written (or already removed) surfaces an error instead of panicking or
// silently returning empty data.
func Test_OPFS_ReadMissing_Errors(t *testing.T) {
	requireOPFSBridge(t)
	const name = "test-does-not-exist.bin"
	_ = opfsRemoveFile(name) // best-effort cleanup from a prior failed run

	if _, err := opfsReadFile(name); err == nil {
		t.Fatal("expected an error reading a file that was never written")
	}
}

// Test_OPFS_Remove verifies a removed file is no longer readable.
func Test_OPFS_Remove(t *testing.T) {
	requireOPFSBridge(t)
	const name = "test-remove.bin"

	if err := opfsWriteFile(name, []byte("x")); err != nil {
		t.Fatalf("opfsWriteFile: %v", err)
	}
	if err := opfsRemoveFile(name); err != nil {
		t.Fatalf("opfsRemoveFile: %v", err)
	}
	if _, err := opfsReadFile(name); err == nil {
		t.Fatal("expected an error reading a removed file")
	}
}

// Test_SnapshotRestore_RoundTrip verifies the demo's actual state (ledger,
// vector docs, agent frames/sessions) survives a snapshot -> mutate ->
// restore cycle intact, which is the core guarantee the OPFS persistence
// feature makes: a killed/reloaded tab gets back exactly what it had.
func Test_SnapshotRestore_RoundTrip(t *testing.T) {
	// Use the real package-level db/agentEngine globals, seeded by init().
	before := snapshotState()
	if len(before.Ledger) == 0 {
		t.Fatal("expected seeded ledger to be non-empty before snapshotting")
	}

	// Mutate state so restore has something real to undo.
	db.mu.Lock()
	db.btree.Add("acc:test-mutation", `{"holder":"Should Be Reverted","balance":1.00}`)
	db.mu.Unlock()

	agentEngine.mu.Lock()
	agentEngine.frames.Add("session-mutation:0", `{"text":"should be reverted"}`)
	agentEngine.mu.Unlock()

	mutated := snapshotState()
	if _, ok := mutated.Ledger["acc:test-mutation"]; !ok {
		t.Fatal("expected mutation to be visible in a fresh snapshot")
	}

	// Restore the original snapshot and confirm the mutation is gone.
	restoreState(before)
	after := snapshotState()

	if _, ok := after.Ledger["acc:test-mutation"]; ok {
		t.Fatal("expected restoreState to revert the ledger mutation")
	}
	if len(after.Ledger) != len(before.Ledger) {
		t.Fatalf("ledger size mismatch after restore: want %d, got %d", len(before.Ledger), len(after.Ledger))
	}
	if len(after.VectorDocs) != len(before.VectorDocs) {
		t.Fatalf("vector doc count mismatch after restore: want %d, got %d", len(before.VectorDocs), len(after.VectorDocs))
	}
	if _, ok := after.AgentFrames["session-mutation:0"]; ok {
		t.Fatal("expected restoreState to revert the agent-memory mutation")
	}
}

// Test_PersistState_HydrateFromOPFS_RoundTrip is the full, real end-to-end
// path: mutate state, persistState() writes it to OPFS via the same JS
// bridge production uses, reset in-memory state to something else entirely,
// then hydrateFromOPFS() reads it back and restores the mutation.
func Test_PersistState_HydrateFromOPFS_RoundTrip(t *testing.T) {
	requireOPFSBridge(t)
	defer opfsRemoveFile(opfsStateFile)

	db.mu.Lock()
	db.btree.Add("acc:opfs-e2e", `{"holder":"OPFS End To End","balance":777.00}`)
	db.mu.Unlock()

	persistState()

	// Blow away in-memory state entirely so hydrateFromOPFS has to actually
	// do the restoring, not just find a no-op.
	restoreState(demoSnapshot{
		Ledger:        map[string]string{},
		AgentFrames:   map[string]string{},
		AgentSessions: map[string]string{},
	})
	if len(snapshotState().Ledger) != 0 {
		t.Fatal("expected ledger to be empty after resetting to a blank snapshot")
	}

	hydrateFromOPFS()

	after := snapshotState()
	v, ok := after.Ledger["acc:opfs-e2e"]
	if !ok {
		t.Fatal("expected hydrateFromOPFS to restore the persisted account")
	}
	if v != `{"holder":"OPFS End To End","balance":777.00}` {
		t.Fatalf("unexpected restored value: %q", v)
	}
}
