package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// --- retryIO coverage -------------------------------------------------------

func Test_retryIO_Table(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name      string
		taskErr   error
		wantNil   bool
		wantRetry bool // whether sop.ShouldRetry(taskErr) is true
	}{
		{name: "nil task", taskErr: nil, wantNil: true},
		{name: "non-retryable error returns lastErr", taskErr: os.ErrPermission, wantNil: false, wantRetry: false},
		{name: "retryable error returns retry error", taskErr: errors.New("transient"), wantNil: false, wantRetry: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// For nil taskErr, simulate success on first try.
			if tt.taskErr == nil {
				if err := retryIO(ctx, func(context.Context) error { return nil }); err != nil {
					t.Fatalf("expected nil, got %v", err)
				}
				return
			}
			// Always return the configured error to exercise both branches.
			err := retryIO(ctx, func(context.Context) error { return tt.taskErr })
			if tt.wantNil {
				if err != nil {
					t.Fatalf("want nil, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			// Sanity: retry path should keep returning a non-nil error from the retry loop.
			if tt.wantRetry && err == nil {
				t.Fatalf("expected retry error, got nil")
			}
		})
	}
}

// --- fileIO with replication: removeStore and replicate coverage -------------

type fakeFileIO struct{ calls []string }

func (f *fakeFileIO) WriteFile(ctx context.Context, name string, data []byte, perm os.FileMode) error {
	f.calls = append(f.calls, "WriteFile:"+name)
	// Create parent dir to avoid errors.
	_ = os.MkdirAll(filepath.Dir(name), 0o755)
	return os.WriteFile(name, data, perm)
}
func (f *fakeFileIO) ReadFile(ctx context.Context, name string) ([]byte, error) {
	f.calls = append(f.calls, "ReadFile:"+name)
	return os.ReadFile(name)
}
func (f *fakeFileIO) Remove(ctx context.Context, name string) error {
	f.calls = append(f.calls, "Remove:"+name)
	return os.Remove(name)
}
func (f *fakeFileIO) Exists(ctx context.Context, path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
func (f *fakeFileIO) RemoveAll(ctx context.Context, path string) error {
	f.calls = append(f.calls, "RemoveAll:"+path)
	return os.RemoveAll(path)
}
func (f *fakeFileIO) MkdirAll(ctx context.Context, path string, perm os.FileMode) error {
	f.calls = append(f.calls, "MkdirAll:"+path)
	return os.MkdirAll(path, perm)
}
func (f *fakeFileIO) ReadDir(ctx context.Context, sourceDir string) ([]os.DirEntry, error) {
	f.calls = append(f.calls, "ReadDir:"+sourceDir)
	return os.ReadDir(sourceDir)
}

func Test_fileIO_removeStore_and_replicate_Table(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	baseA := filepath.Join(t.TempDir(), "a")
	baseB := filepath.Join(t.TempDir(), "b")
	rt, err := NewReplicationTracker(ctx, []string{baseA, baseB}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}

	// Inject our fake FileIO
	old := FileIOSim
	ff := &fakeFileIO{}
	FileIOSim = ff
	t.Cleanup(func() { FileIOSim = old })

	tests := []struct {
		name         string
		trackActions bool
		expectTrack  bool
	}{
		{name: "no tracking", trackActions: false, expectTrack: false},
		{name: "with tracking", trackActions: true, expectTrack: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Fresh instance each subtest to avoid shared actionsDone.
			fio := newFileIOWithReplication(rt, NewManageStoreFolder(NewFileIO()), tt.trackActions)
			folder := "store-x"
			// Ensure active folder exists so RemoveAll runs against a real path (ok if missing).
			_ = os.MkdirAll(rt.formatActiveFolderEntity(folder), 0o755)
			if err := fio.removeStore(ctx, folder); err != nil {
				t.Fatalf("removeStore err: %v", err)
			}
			if tt.expectTrack {
				if len(fio.actionsDone) != 1 || fio.actionsDone[0].First != 3 {
					t.Fatalf("expected one remove action recorded, got %+v", fio.actionsDone)
				}
				// Replicate should attempt RemoveAll on passive path and clear actions.
				if err := fio.replicate(ctx); err != nil {
					t.Fatalf("replicate err: %v", err)
				}
				if len(fio.actionsDone) != 0 {
					t.Fatalf("expected actions cleared after replicate")
				}
			} else {
				if len(fio.actionsDone) != 0 {
					t.Fatalf("did not expect actions tracked, got %+v", fio.actionsDone)
				}
			}
		})
	}
}

// --- updateFileBlockRegion lock timeout branch -------------------------------

// lockFailingCache wraps a base mock cache to force lock acquisition to fail continuously
// and simulate IsLocked returning false. Other methods delegate to base.
type lockFailingCache struct{ base sop.Cache }

func (m *lockFailingCache) Set(ctx context.Context, k, v string, d time.Duration) error {
	return m.base.Set(ctx, k, v, d)
}
func (m *lockFailingCache) Get(ctx context.Context, k string) (bool, string, error) {
	return m.base.Get(ctx, k)
}
func (m *lockFailingCache) GetEx(ctx context.Context, k string, d time.Duration) (bool, string, error) {
	return m.base.GetEx(ctx, k, d)
}
func (m *lockFailingCache) Ping(ctx context.Context) error { return nil }
func (m *lockFailingCache) SetStruct(ctx context.Context, k string, v interface{}, d time.Duration) error {
	return m.base.SetStruct(ctx, k, v, d)
}
func (m *lockFailingCache) GetStruct(ctx context.Context, k string, v interface{}) (bool, error) {
	return m.base.GetStruct(ctx, k, v)
}
func (m *lockFailingCache) GetStructEx(ctx context.Context, k string, v interface{}, d time.Duration) (bool, error) {
	return m.base.GetStructEx(ctx, k, v, d)
}
func (m *lockFailingCache) Delete(ctx context.Context, ks []string) (bool, error) {
	return m.base.Delete(ctx, ks)
}
func (m *lockFailingCache) FormatLockKey(k string) string { return m.base.FormatLockKey(k) }
func (m *lockFailingCache) CreateLockKeys(keys []string) []*sop.LockKey {
	return m.base.CreateLockKeys(keys)
}
func (m *lockFailingCache) CreateLockKeysForIDs(keys []sop.Tuple[string, sop.UUID]) []*sop.LockKey {
	return m.base.CreateLockKeysForIDs(keys)
}
func (m *lockFailingCache) IsLockedTTL(ctx context.Context, d time.Duration, lks []*sop.LockKey) (bool, error) {
	return false, nil
}
func (m *lockFailingCache) Lock(ctx context.Context, d time.Duration, lks []*sop.LockKey) (bool, sop.UUID, error) {
	return false, sop.NilUUID, nil
}
func (m *lockFailingCache) IsLocked(ctx context.Context, lks []*sop.LockKey) (bool, error) {
	return false, nil
}
func (m *lockFailingCache) IsLockedByOthers(ctx context.Context, ks []string) (bool, error) {
	return m.base.IsLockedByOthers(ctx, ks)
}
func (m *lockFailingCache) Unlock(ctx context.Context, lks []*sop.LockKey) error { return nil }
func (m *lockFailingCache) Clear(ctx context.Context) error                      { return m.base.Clear(ctx) }
func Test_updateFileBlockRegion_LockTimeout(t *testing.T) {
	t.Parallel()
	// Short-deadline context forces the timeout branch on lock acquisition loop.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	base := t.TempDir()
	rt, err := NewReplicationTracker(context.Background(), []string{base}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	hm := &hashmap{hashModValue: MinimumModValue, replicationTracker: rt, readWrite: true, fileHandles: make(map[string]*fileDirectIO), cache: &lockFailingCache{base: mocks.NewMockClient()}}

	dio := newFileDirectIO()
	dio.filename = filepath.Join(base, "seg-1.reg") // only used for lock key formatting

	err = hm.updateFileBlockRegion(ctx, dio, 0, 0, make([]byte, sop.HandleSizeInBytes))
	// Expect a sop.Error with LockAcquisitionFailure code
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	var se sop.Error
	if !errors.As(err, &se) || se.Code != sop.LockAcquisitionFailure {
		t.Fatalf("expected sop.Error with LockAcquisitionFailure, got %T %v", err, err)
	}
}

// --- getFilesSortedDescByModifiedTime error path ----------------------------

func Test_getFilesSortedDescByModifiedTime_ErrorOnFilePath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// Create a regular file where a directory is expected; ReadDir should fail
	temp := t.TempDir()
	p := filepath.Join(temp, "not-a-dir")
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Call with directoryPath pointing to a file
	_, err := getFilesSortedDescByModifiedTime(ctx, p, ".log", nil)
	if err == nil {
		t.Fatalf("expected error reading non-directory path, got nil")
	}
}
