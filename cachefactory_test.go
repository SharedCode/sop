package sop

import (
	"context"
	"testing"
	"time"
)

func TestGetL2Cache_RedisInstances(t *testing.T) {
	// Save original registry and instances
	l2locker.Lock()
	originalRegistry := make(map[L2CacheType]L2CacheFactory)
	for k, v := range cacheRegistry {
		originalRegistry[k] = v
	}
	originalInstances := make(map[string]L2Cache)
	for k, v := range cacheInstances {
		originalInstances[k] = v
	}
	// Clear instances for test
	cacheInstances = make(map[string]L2Cache)
	l2locker.Unlock()

	defer func() {
		l2locker.Lock()
		cacheRegistry = originalRegistry
		cacheInstances = originalInstances
		l2locker.Unlock()
	}()

	// Register a dummy factory that returns a new object each time
	RegisterL2CacheFactory(Redis, func(opts TransactionOptions) L2Cache {
		return &mockL2Cache{}
	})

	// Config 1
	opts1 := TransactionOptions{
		CacheType: Redis,
		RedisConfig: &RedisCacheConfig{
			Address: "localhost:6379",
			DB:      0,
		},
	}

	// Config 2 (Same as 1)
	opts2 := TransactionOptions{
		CacheType: Redis,
		RedisConfig: &RedisCacheConfig{
			Address: "localhost:6379",
			DB:      0,
		},
	}

	// Config 3 (Different DB)
	opts3 := TransactionOptions{
		CacheType: Redis,
		RedisConfig: &RedisCacheConfig{
			Address: "localhost:6379",
			DB:      1,
		},
	}

	c1 := GetL2Cache(opts1)
	c2 := GetL2Cache(opts2)
	c3 := GetL2Cache(opts3)

	t.Logf("c1: %p, key: %s", c1, getCacheKey(opts1))
	t.Logf("c2: %p, key: %s", c2, getCacheKey(opts2))
	t.Logf("c3: %p, key: %s", c3, getCacheKey(opts3))

	if c1 == nil || c2 == nil || c3 == nil {
		t.Fatal("GetL2Cache returned nil")
	}

	if c1 != c2 {
		t.Error("Expected c1 and c2 to be the same instance (same config)")
	}

	if c1 == c3 {
		t.Error("Expected c1 and c3 to be different instances (different DB)")
	}
}

// Minimal mock implementation
type mockL2Cache struct {
	dummy int
}

func (m *mockL2Cache) GetType() L2CacheType { return Redis }
func (m *mockL2Cache) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	return nil
}
func (m *mockL2Cache) Get(ctx context.Context, key string) (bool, string, error) {
	return false, "", nil
}
func (m *mockL2Cache) GetEx(ctx context.Context, key string, expiration time.Duration) (bool, string, error) {
	return false, "", nil
}
func (m *mockL2Cache) IsRestarted(ctx context.Context) bool { return false }
func (m *mockL2Cache) SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return nil
}
func (m *mockL2Cache) SetStructs(ctx context.Context, keys []string, values []interface{}, expiration time.Duration) error {
	return nil
}
func (m *mockL2Cache) GetStruct(ctx context.Context, key string, target interface{}) (bool, error) {
	return false, nil
}
func (m *mockL2Cache) GetStructEx(ctx context.Context, key string, target interface{}, expiration time.Duration) (bool, error) {
	return false, nil
}
func (m *mockL2Cache) GetStructs(ctx context.Context, keys []string, targets []interface{}, expiration time.Duration) ([]bool, error) {
	return make([]bool, len(keys)), nil
}
func (m *mockL2Cache) Delete(ctx context.Context, keys []string) (bool, error) { return true, nil }
func (m *mockL2Cache) Ping(ctx context.Context) error                          { return nil }
func (m *mockL2Cache) FormatLockKey(k string) string                           { return k }
func (m *mockL2Cache) CreateLockKeys(keys []string) []*LockKey                 { return nil }
func (m *mockL2Cache) CreateLockKeysForIDs(keys []Tuple[string, UUID]) []*LockKey {
	return nil
}
func (m *mockL2Cache) IsLockedTTL(ctx context.Context, duration time.Duration, lockKeys []*LockKey) (bool, error) {
	return false, nil
}
func (m *mockL2Cache) Lock(ctx context.Context, duration time.Duration, lockKeys []*LockKey) (bool, UUID, error) {
	return true, UUID{}, nil
}
func (m *mockL2Cache) DualLock(ctx context.Context, duration time.Duration, lockKeys []*LockKey) (bool, UUID, error) {
	return true, UUID{}, nil
}
func (m *mockL2Cache) IsLocked(ctx context.Context, lockKeys []*LockKey) (bool, error) {
	return false, nil
}
func (m *mockL2Cache) IsLockedByOthers(ctx context.Context, lockKeyNames []string) (bool, error) {
	return false, nil
}
func (m *mockL2Cache) IsLockedByOthersTTL(ctx context.Context, lockKeyNames []string, duration time.Duration) (bool, error) {
	return false, nil
}
func (m *mockL2Cache) Unlock(ctx context.Context, lockKeys []*LockKey) error { return nil }
func (m *mockL2Cache) Clear(ctx context.Context) error                       { return nil }
