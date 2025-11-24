package cache

import (
	"context"
	"testing"
	"time"
)

func TestInMemoryCache_BasicOperations(t *testing.T) {
	c := NewInMemoryCache()
	ctx := context.Background()

	// Test Set and Get
	key := "testKey"
	value := "testValue"
	err := c.Set(ctx, key, value, time.Minute)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	found, val, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatalf("Get returned not found")
	}
	if val != value {
		t.Errorf("Get returned %s, expected %s", val, value)
	}

	// Test Delete
	deleted, err := c.Delete(ctx, []string{key})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !deleted {
		t.Errorf("Delete returned false")
	}

	found, _, err = c.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	if found {
		t.Errorf("Get after delete returned found")
	}
}

func TestInMemoryCache_Expiration(t *testing.T) {
	c := NewInMemoryCache()
	ctx := context.Background()

	key := "expKey"
	value := "expValue"
	// Set with short expiration
	err := c.Set(ctx, key, value, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Should be found immediately
	found, _, _ := c.Get(ctx, key)
	if !found {
		t.Fatalf("Get returned not found immediately after Set")
	}

	// Wait for expiration
	time.Sleep(200 * time.Millisecond)

	found, _, _ = c.Get(ctx, key)
	if found {
		t.Errorf("Get returned found after expiration")
	}
}

func TestInMemoryCache_StructOperations(t *testing.T) {
	c := NewInMemoryCache()
	ctx := context.Background()

	type TestStruct struct {
		Name string
		Age  int
	}

	key := "structKey"
	val := TestStruct{Name: "John", Age: 30}

	err := c.SetStruct(ctx, key, val, time.Minute)
	if err != nil {
		t.Fatalf("SetStruct failed: %v", err)
	}

	var result TestStruct
	found, err := c.GetStruct(ctx, key, &result)
	if err != nil {
		t.Fatalf("GetStruct failed: %v", err)
	}
	if !found {
		t.Fatalf("GetStruct returned not found")
	}
	if result != val {
		t.Errorf("GetStruct returned %+v, expected %+v", result, val)
	}
}

func TestInMemoryCache_Locking(t *testing.T) {
	c := NewInMemoryCache()
	ctx := context.Background()

	keys := []string{"lock1", "lock2"}
	lockKeys := c.CreateLockKeys(keys)

	if len(lockKeys) != 2 {
		t.Fatalf("CreateLockKeys returned %d keys, expected 2", len(lockKeys))
	}

	// Test Lock
	ok, _, err := c.Lock(ctx, time.Minute, lockKeys)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	if !ok {
		t.Errorf("Lock returned false")
	}

	// Test IsLocked
	ok, err = c.IsLocked(ctx, lockKeys)
	if err != nil {
		t.Fatalf("IsLocked failed: %v", err)
	}
	if !ok {
		t.Errorf("IsLocked returned false")
	}

	// Test Unlock
	err = c.Unlock(ctx, lockKeys)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func TestInMemoryCache_GetEx(t *testing.T) {
	c := NewInMemoryCache()
	ctx := context.Background()

	key := "getExKey"
	value := "getExValue"

	// Set with very short expiration
	err := c.Set(ctx, key, value, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// GetEx with longer expiration to extend it
	found, val, err := c.GetEx(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("GetEx failed: %v", err)
	}
	if !found {
		t.Fatalf("GetEx returned not found")
	}
	if val != value {
		t.Errorf("GetEx returned %s, expected %s", val, value)
	}

	// Wait for original expiration time
	time.Sleep(300 * time.Millisecond)

	// Should still be there because GetEx extended it
	found, _, _ = c.Get(ctx, key)
	if !found {
		t.Errorf("Get returned not found after GetEx extension")
	}
}
