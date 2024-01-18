package redis

import (
	"context"
	"time"
)

type mockRedis struct {
	lookup map[string][]byte
}

// Returns a new Redis mock client.
func NewMockClient() Cache {
	return &mockRedis{}
}

// Unused in SOP in_red_ck package, 'stubs only for now.
func (m mockRedis) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	return nil
}
func (m mockRedis) Get(ctx context.Context, key string) (string, error) {
	return "", nil
}
func (m mockRedis) Ping(ctx context.Context) error {
	return nil
}

// Mocks.
func (m *mockRedis) SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	// serialize User object to JSON
	ba, err := Marshaler.Marshal(value)
	if err != nil {
		return err
	}
	m.lookup[key] = ba
	return nil
}

func (m *mockRedis) GetStruct(ctx context.Context, key string, target interface{}) error {
	ba, ok := m.lookup[key]
	if ok {
		Marshaler.Unmarshal(ba, target)
	}
	return nil
}
func (m *mockRedis) Delete(ctx context.Context, keys ...string) error {
	for _, k := range keys {
		delete(m.lookup, k)
	}
	return nil
}
