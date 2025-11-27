package common

import (
	"testing"

	"github.com/sharedcode/sop"
)

func init() {
	// Register dummy factories to allow SetCacheFactory to work
	sop.RegisterCacheFactory(sop.NoCache, func() sop.L2Cache { return nil })
	sop.RegisterCacheFactory(sop.InMemory, func() sop.L2Cache { return nil })
	sop.RegisterCacheFactory(sop.Redis, func() sop.L2Cache { return nil })
}

func TestOnStartUp_InMemory(t *testing.T) {
	// Save state
	originalFlag := onStartUpFlag
	originalFactory := sop.GetCacheFactoryType()
	defer func() {
		onStartUpFlag = originalFlag
		sop.SetCacheFactory(originalFactory)
	}()

	// Setup
	onStartUpFlag = true
	sop.SetCacheFactory(sop.InMemory)

	// First call should return true
	if !onStartUp() {
		t.Error("Expected onStartUp() to return true on first call for InMemory, got false")
	}

	// Second call should return false (run once)
	if onStartUp() {
		t.Error("Expected onStartUp() to return false on second call for InMemory, got true")
	}
}

func TestOnStartUp_NoCache(t *testing.T) {
	// Save state
	originalFlag := onStartUpFlag
	originalFactory := sop.GetCacheFactoryType()
	defer func() {
		onStartUpFlag = originalFlag
		sop.SetCacheFactory(originalFactory)
	}()

	// Setup
	onStartUpFlag = true
	sop.SetCacheFactory(sop.NoCache)

	// Should return false
	if onStartUp() {
		t.Error("Expected onStartUp() to return false for NoCache, got true")
	}
}

func TestOnStartUp_Redis(t *testing.T) {
	// Save state
	originalFlag := onStartUpFlag
	originalFactory := sop.GetCacheFactoryType()
	defer func() {
		onStartUpFlag = originalFlag
		sop.SetCacheFactory(originalFactory)
	}()

	// Setup
	onStartUpFlag = true
	sop.SetCacheFactory(sop.Redis)

	// Should return false
	if onStartUp() {
		t.Error("Expected onStartUp() to return false for Redis, got true")
	}
}
