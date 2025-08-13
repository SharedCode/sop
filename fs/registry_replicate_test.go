package fs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// cacheGetError wraps a mock cache to induce an error on the first GetStruct/Ex call.
type cacheGetError struct {
	base    sop.Cache
	tripped bool
}

func newCacheGetError() *cacheGetError { return &cacheGetError{base: mocks.NewMockClient()} }

func (c *cacheGetError) Set(ctx context.Context, k, v string, d time.Duration) error {
	return c.base.Set(ctx, k, v, d)
}
func (c *cacheGetError) Get(ctx context.Context, k string) (bool, string, error) {
	return c.base.Get(ctx, k)
}
func (c *cacheGetError) GetEx(ctx context.Context, k string, d time.Duration) (bool, string, error) {
	return c.base.GetEx(ctx, k, d)
}
func (c *cacheGetError) Ping(ctx context.Context) error { return nil }
func (c *cacheGetError) SetStruct(ctx context.Context, k string, v interface{}, d time.Duration) error {
	return c.base.SetStruct(ctx, k, v, d)
}
func (c *cacheGetError) GetStruct(ctx context.Context, k string, v interface{}) (bool, error) {
	if !c.tripped {
		c.tripped = true
		return false, fmt.Errorf("induced getstruct error")
	}
	return c.base.GetStruct(ctx, k, v)
}
func (c *cacheGetError) GetStructEx(ctx context.Context, k string, v interface{}, d time.Duration) (bool, error) {
	if !c.tripped {
		c.tripped = true
		return false, fmt.Errorf("induced getstructex error")
	}
	return c.base.GetStructEx(ctx, k, v, d)
}
func (c *cacheGetError) Delete(ctx context.Context, ks []string) (bool, error) {
	return c.base.Delete(ctx, ks)
}
func (c *cacheGetError) FormatLockKey(k string) string { return c.base.FormatLockKey(k) }
func (c *cacheGetError) CreateLockKeys(keys []string) []*sop.LockKey {
	return c.base.CreateLockKeys(keys)
}
func (c *cacheGetError) CreateLockKeysForIDs(keys []sop.Tuple[string, sop.UUID]) []*sop.LockKey {
	return c.base.CreateLockKeysForIDs(keys)
}
func (c *cacheGetError) IsLockedTTL(ctx context.Context, d time.Duration, lks []*sop.LockKey) (bool, error) {
	return c.base.IsLockedTTL(ctx, d, lks)
}
func (c *cacheGetError) Lock(ctx context.Context, d time.Duration, lks []*sop.LockKey) (bool, sop.UUID, error) {
	return c.base.Lock(ctx, d, lks)
}
func (c *cacheGetError) IsLocked(ctx context.Context, lks []*sop.LockKey) (bool, error) {
	return c.base.IsLocked(ctx, lks)
}
func (c *cacheGetError) IsLockedByOthers(ctx context.Context, ks []string) (bool, error) {
	return c.base.IsLockedByOthers(ctx, ks)
}
func (c *cacheGetError) Unlock(ctx context.Context, lks []*sop.LockKey) error {
	return c.base.Unlock(ctx, lks)
}
func (c *cacheGetError) Clear(ctx context.Context) error { return c.base.Clear(ctx) }

// TestRegistry_Get_AllFound_NoFetch exercises the fast-path where all IDs are found in L2 cache (no disk fetch).
func TestRegistry_Get_AllFound_NoFetch(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
	r := NewRegistry(true, MinimumModValue, rt, l2)
	defer r.Close()
	h1 := sop.NewHandle(sop.NewUUID())
	h2 := sop.NewHandle(sop.NewUUID())
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "geta", IDs: []sop.Handle{h1, h2}}}); err != nil {
		t.Fatalf("add: %v", err)
	}
	res, err := r.Get(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "geta", IDs: []sop.UUID{h1.LogicalID, h2.LogicalID}}})
	if err != nil || len(res) != 1 || len(res[0].IDs) != 2 {
		t.Fatalf("unexpected get result: %v %+v", err, res)
	}
}

// TestRegistry_Get_ErrorOnCacheGet covers log/continue branch when L2 cache get returns an error.
func TestRegistry_Get_ErrorOnCacheGet(t *testing.T) {
	ctx := context.Background()
	cg := newCacheGetError()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, cg)
	r := NewRegistry(true, MinimumModValue, rt, cg)
	defer r.Close()
	h := sop.NewHandle(sop.NewUUID())
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "geterr", IDs: []sop.Handle{h}}}); err != nil {
		t.Fatalf("add: %v", err)
	}
	res, err := r.Get(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: "geterr", IDs: []sop.UUID{h.LogicalID}}})
	if err != nil || len(res) != 1 || len(res[0].IDs) != 1 {
		t.Fatalf("unexpected get result after induced cache error: %v %+v", err, res)
	}
}

// TestRegistry_Replicate_CloseOverrideErrors validates rmCloseOverride error handling with and without prior errors.
func TestRegistry_Replicate_CloseOverrideErrors(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	a := t.TempDir()
	b := t.TempDir()
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
	rt, _ := NewReplicationTracker(ctx, []string{a, b}, true, l2)
	r := NewRegistry(true, MinimumModValue, rt, l2)
	defer r.Close()
	h1 := sop.NewHandle(sop.NewUUID())
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "replc", IDs: []sop.Handle{h1}}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	r.rmCloseOverride = func() error { return fmt.Errorf("close override error") }
	if err := r.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "replc", IDs: []sop.Handle{h1}}}, nil, nil, nil); err == nil || err.Error() != "close override error" {
		t.Fatalf("expected close override error, got %v", err)
	}
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
	a2 := t.TempDir()
	passiveFile := filepath.Join(t.TempDir(), "pas-file")
	if err := os.WriteFile(passiveFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	rt2, _ := NewReplicationTracker(ctx, []string{a2, passiveFile}, true, l2)
	r2 := NewRegistry(true, MinimumModValue, rt2, l2)
	defer r2.Close()
	h2 := sop.NewHandle(sop.NewUUID())
	if err := r2.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "replcerr", IDs: []sop.Handle{h2}}}); err != nil {
		t.Fatalf("seed2: %v", err)
	}
	r2.rmCloseOverride = func() error { return fmt.Errorf("ignored close error") }
	if err := r2.Replicate(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "replcerr", IDs: []sop.Handle{h2}}}, nil, nil, nil); err == nil || err.Error() == "ignored close error" {
		t.Fatalf("expected earlier replication add error, got %v", err)
	}
}

// TestRegistry_Replicate_LayeredErrors forces an early failure (set/update) and a secondary rmCloseOverride error;
// verifies first error wins and FailedToReplicate flag flips.
func TestRegistry_Replicate_LayeredErrors(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	active := t.TempDir()
	passiveDir := t.TempDir()
	// Make passive path a file to trigger replication add/set errors when attempting to write there.
	passiveFile := filepath.Join(passiveDir, "pasfile")
	if err := os.WriteFile(passiveFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("seed passive file: %v", err)
	}
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
	rt, _ := NewReplicationTracker(ctx, []string{active, passiveFile}, true, cache)
	r := NewRegistry(true, MinimumModValue, rt, cache)
	defer r.Close()

	// Seed initial handle to allow an update scenario.
	hSeed := sop.NewHandle(sop.NewUUID())
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rlay", IDs: []sop.Handle{hSeed}}}); err != nil {
		t.Fatalf("seed add: %v", err)
	}
	// Prepare payloads: new root, added, updated (same lid w/ version bump), removed.
	newRoot := sop.NewHandle(sop.NewUUID())
	updated := hSeed
	updated.Version = 2
	remove := sop.NewHandle(sop.NewUUID()) // removing non-existent different lid triggers remove failure after earlier failures handled.

	// Force close override secondary error.
	closeErr := errors.New("close override error")
	r.rmCloseOverride = func() error { return closeErr }

	err := r.Replicate(ctx,
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rlay", IDs: []sop.Handle{newRoot}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rlay", IDs: []sop.Handle{hSeed}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rlay", IDs: []sop.Handle{updated}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rlay", IDs: []sop.Handle{remove}}},
	)
	if err == nil {
		t.Fatalf("expected primary replication error")
	}
	if !rt.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate set")
	}
	if err.Error() == closeErr.Error() {
		t.Fatalf("expected first replication error to win, got close override only")
	}
}
