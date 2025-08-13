package mocks

import (
	"context"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/encoding"
)

type mockRedis struct {
	lookup      map[string][]byte // for SetStruct/GetStruct
	stringStore map[string]string // for Set/Get and locking values
}

// Returns a new Redis mock client.
func NewMockClient() sop.Cache {
	return &mockRedis{
		lookup:      make(map[string][]byte),
		stringStore: make(map[string]string),
	}
}

// String operations used by locking implementation.
func (m *mockRedis) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	m.stringStore[key] = value
	return nil
}
func (m *mockRedis) Get(ctx context.Context, key string) (bool, string, error) {
	v, ok := m.stringStore[key]
	if !ok {
		return false, "", nil
	}
	return true, v, nil
}
func (m *mockRedis) GetEx(ctx context.Context, key string, expiration time.Duration) (bool, string, error) {
	// Ignore TTL in mock; behave like Get.
	return m.Get(ctx, key)
}
func (m *mockRedis) Ping(ctx context.Context) error { return nil }

// Struct operations used by value caching and item locks.
func (m *mockRedis) SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	ba, err := encoding.BlobMarshaler.Marshal(value)
	if err != nil {
		return err
	}
	m.lookup[key] = ba
	return nil
}

func (m *mockRedis) GetStruct(ctx context.Context, key string, target interface{}) (bool, error) {
	ba, ok := m.lookup[key]
	if !ok {
		// Real client returns (false, nil) when key not found.
		return false, nil
	}
	encoding.BlobMarshaler.Unmarshal(ba, target)
	return true, nil
}

// Mock only support GetStruct; GetStructEx just calls GetStruct ignoring expiration.
func (m *mockRedis) GetStructEx(ctx context.Context, key string, target interface{}, expiration time.Duration) (bool, error) {
	return m.GetStruct(ctx, key, target)
}

// Delete removes keys from both string and struct maps.
func (m *mockRedis) Delete(ctx context.Context, keys []string) (bool, error) {
	deletedAny := false
	for _, k := range keys {
		if _, ok := m.stringStore[k]; ok {
			delete(m.stringStore, k)
			deletedAny = true
		}
		if _, ok := m.lookup[k]; ok {
			delete(m.lookup, k)
			deletedAny = true
		}
	}
	return deletedAny, nil
}

// Lock key helpers compatible with real redis client.
func (c *mockRedis) FormatLockKey(k string) string { return "L" + k }

func (m *mockRedis) CreateLockKeys(keys []string) []*sop.LockKey {
	lockKeys := make([]*sop.LockKey, len(keys))
	for i := range keys {
		lockKeys[i] = &sop.LockKey{
			Key:    m.FormatLockKey(keys[i]),
			LockID: sop.NewUUID(),
		}
	}
	return lockKeys
}
func (m *mockRedis) CreateLockKeysForIDs(keys []sop.Tuple[string, sop.UUID]) []*sop.LockKey {
	lockKeys := make([]*sop.LockKey, len(keys))
	for i := range keys {
		lockKeys[i] = &sop.LockKey{
			Key:    m.FormatLockKey(keys[i].First),
			LockID: keys[i].Second,
		}
	}
	return lockKeys
}

func (m *mockRedis) IsLockedTTL(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, error) {
	r := true
	for _, lk := range lockKeys {
		if v, ok := m.stringStore[lk.Key]; ok && v == lk.LockID.String() {
			lk.IsLockOwner = true
			continue
		}
		lk.IsLockOwner = false
		r = false
	}
	return r, nil
}

func (m *mockRedis) Lock(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, sop.UUID, error) {
	// Try to acquire all locks; if any conflict is found, return false with owner UUID.
	for _, lk := range lockKeys {
		if v, ok := m.stringStore[lk.Key]; ok {
			if v != lk.LockID.String() {
				id, _ := sop.ParseUUID(v)
				return false, id, nil
			}
			// already ours; skip
			continue
		}
		// Not present: acquire
		m.stringStore[lk.Key] = lk.LockID.String()
		lk.IsLockOwner = true
	}
	return true, sop.NilUUID, nil
}

func (m *mockRedis) IsLocked(ctx context.Context, lockKeys []*sop.LockKey) (bool, error) {
	r := true
	for _, lk := range lockKeys {
		if v, ok := m.stringStore[lk.Key]; ok && v == lk.LockID.String() {
			lk.IsLockOwner = true
			continue
		}
		lk.IsLockOwner = false
		r = false
	}
	return r, nil
}

func (m *mockRedis) IsLockedByOthers(ctx context.Context, lockKeyNames []string) (bool, error) {
	if len(lockKeyNames) == 0 {
		return false, nil
	}
	for _, k := range lockKeyNames {
		if _, ok := m.stringStore[k]; !ok {
			return false, nil
		}
	}
	return true, nil
}

func (m *mockRedis) Unlock(ctx context.Context, lockKeys []*sop.LockKey) error {
	for _, lk := range lockKeys {
		if !lk.IsLockOwner {
			continue
		}
		if v, ok := m.stringStore[lk.Key]; ok && v == lk.LockID.String() {
			delete(m.stringStore, lk.Key)
		}
	}
	return nil
}

func (m *mockRedis) Clear(ctx context.Context) error {
	m.lookup = make(map[string][]byte)
	m.stringStore = make(map[string]string)
	return nil
}
