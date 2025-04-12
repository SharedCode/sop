package mocks

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
)

type mockRedis struct {
	lookup map[string][]byte
}

// Returns a new Redis mock client.
func NewMockClient() sop.Cache {
	return &mockRedis{
		lookup: make(map[string][]byte),
	}
}

// Unused in SOP in_red_ck package, 'stubs only for now.
func (m mockRedis) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	return nil
}
func (m mockRedis) Get(ctx context.Context, key string) (string, error) {
	return "", nil
}
func (m mockRedis) GetEx(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return "", nil
}
func (m mockRedis) Ping(ctx context.Context) error {
	return nil
}

// Mocks.
func (m *mockRedis) SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	// serialize User object to JSON
	ba, err := encoding.BlobMarshaler.Marshal(value)
	if err != nil {
		return err
	}
	m.lookup[key] = ba
	return nil
}

func (m *mockRedis) GetStruct(ctx context.Context, key string, target interface{}) error {
	ba, ok := m.lookup[key]
	if !ok {
		return redis.Nil
	}
	encoding.BlobMarshaler.Unmarshal(ba, target)
	return nil
}

// Mock only support GetStruct, GetStructEx just calls GetStruct ignoring expiration.
func (m *mockRedis) GetStructEx(ctx context.Context, key string, target interface{}, expiration time.Duration) error {
	return m.GetStruct(ctx, key, target)
}
func (m *mockRedis) Delete(ctx context.Context, keys ...string) error {
	var lastErr error
	for _, k := range keys {
		if _, ok := m.lookup[k]; !ok {
			lastErr = redis.Nil
			continue
		}
		delete(m.lookup, k)
	}
	return lastErr
}


// Unimplemented and is not used in this mock.

func (c *mockRedis)FormatLockKey(k string) string {
	return k
}

func (m *mockRedis)KeyNotFound(err error) bool {
	return false
}

func (m *mockRedis)CreateLockKeys(keys ...string) []*sop.LockKeys {
	return nil
}

func (m *mockRedis)Lock(ctx context.Context, duration time.Duration, lockKeys ...*sop.LockKeys) error {
	return nil
}

func (m *mockRedis)IsLocked(ctx context.Context, lockKeys ...*sop.LockKeys) error {
	return nil
}

func (m *mockRedis)Unlock(ctx context.Context, lockKeys ...*sop.LockKeys) error {
	return nil
}
