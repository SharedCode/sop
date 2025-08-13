package fs

import (
    "context"
    "errors"
    "os"
    "path/filepath"
    "time"

    "github.com/sharedcode/sop"
)

type fakeDirectIO struct{}
func (f *fakeDirectIO) Open(ctx context.Context, filename string, flag int, perm os.FileMode) (*os.File, error) { if dir := filepath.Dir(filename); dir != "." { _ = os.MkdirAll(dir, perm) }; return os.OpenFile(filename, flag, perm) }
func (f *fakeDirectIO) WriteAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) { return file.WriteAt(block, offset) }
func (f *fakeDirectIO) ReadAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) { return file.ReadAt(block, offset) }
func (f *fakeDirectIO) Close(file *os.File) error { return file.Close() }

type failingDirectIO struct { openErr error; partialRead bool; partialWrite bool }
func (f *failingDirectIO) Open(ctx context.Context, filename string, flag int, perm os.FileMode) (*os.File, error) { if f.openErr != nil { return nil, f.openErr }; if dir := filepath.Dir(filename); dir != "." { _ = os.MkdirAll(dir, perm) }; return os.OpenFile(filename, flag, perm) }
func (f *failingDirectIO) WriteAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) { if f.partialWrite { return len(block)-10, nil }; return file.WriteAt(block, offset) }
func (f *failingDirectIO) ReadAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) { if f.partialRead { return len(block)-20, nil }; return file.ReadAt(block, offset) }
func (f *failingDirectIO) Close(file *os.File) error { return file.Close() }

type mockCacheHashmap struct { base sop.Cache; lockFail bool; isLockedAlways bool }
func (m *mockCacheHashmap) Lock(ctx context.Context, d time.Duration, lk []*sop.LockKey) (bool, sop.UUID, error) { if m.lockFail { return false, sop.NilUUID, nil }; return m.base.Lock(ctx, d, lk) }
func (m *mockCacheHashmap) IsLocked(ctx context.Context, lk []*sop.LockKey) (bool, error) { if m.isLockedAlways { return true, nil }; return m.base.IsLocked(ctx, lk) }
func (m *mockCacheHashmap) Set(ctx context.Context, key, value string, exp time.Duration) error { return m.base.Set(ctx, key, value, exp) }
func (m *mockCacheHashmap) Get(ctx context.Context, key string) (bool, string, error) { return m.base.Get(ctx, key) }
func (m *mockCacheHashmap) GetEx(ctx context.Context, key string, exp time.Duration) (bool, string, error) { return m.base.GetEx(ctx, key, exp) }
func (m *mockCacheHashmap) Ping(ctx context.Context) error { return m.base.Ping(ctx) }
func (m *mockCacheHashmap) SetStruct(ctx context.Context, key string, v interface{}, exp time.Duration) error { return m.base.SetStruct(ctx, key, v, exp) }
func (m *mockCacheHashmap) GetStruct(ctx context.Context, key string, target interface{}) (bool, error) { return m.base.GetStruct(ctx, key, target) }
func (m *mockCacheHashmap) GetStructEx(ctx context.Context, key string, target interface{}, exp time.Duration) (bool, error) { return m.base.GetStructEx(ctx, key, target, exp) }
func (m *mockCacheHashmap) Delete(ctx context.Context, keys []string) (bool, error) { return m.base.Delete(ctx, keys) }
func (m *mockCacheHashmap) FormatLockKey(k string) string { return m.base.FormatLockKey(k) }
func (m *mockCacheHashmap) CreateLockKeys(keys []string) []*sop.LockKey { return m.base.CreateLockKeys(keys) }
func (m *mockCacheHashmap) CreateLockKeysForIDs(keys []sop.Tuple[string, sop.UUID]) []*sop.LockKey { return m.base.CreateLockKeysForIDs(keys) }
func (m *mockCacheHashmap) IsLockedTTL(ctx context.Context, d time.Duration, lk []*sop.LockKey) (bool, error) { return m.base.IsLockedTTL(ctx, d, lk) }
func (m *mockCacheHashmap) IsLockedByOthers(ctx context.Context, names []string) (bool, error) { return m.base.IsLockedByOthers(ctx, names) }
func (m *mockCacheHashmap) Unlock(ctx context.Context, lk []*sop.LockKey) error { return m.base.Unlock(ctx, lk) }
func (m *mockCacheHashmap) Clear(ctx context.Context) error { return m.base.Clear(ctx) }

func contains(s, sub string) bool { return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0) }
func indexOf(s, sub string) int { if len(sub) == 0 { return 0 }; max := len(s)-len(sub)+1; for i:=0;i<max;i++ { if s[i]==sub[0] && s[i:i+len(sub)]==sub { return i } }; return -1 }

var _ = errors.New
