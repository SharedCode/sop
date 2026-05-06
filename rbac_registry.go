package sop

import (
	"context"
	"sync"
)

// AssetBlueprint defines the RBAC footprint for a specific module or asset type.
type AssetBlueprint struct {
	AssetType   string   // e.g., "space", "store", "agent"
	Description string   // Human-readable context
	Endpoints   []string // UI API endpoints related to this asset
	Actions     []Action // Capabilities: Read, Write, Delete, Execute

	// Evaluator executes the actual permission check for a specific asset instance or context
	Evaluator func(ctx context.Context, entitlementCtx EntitlementContext, action Action) bool
}

var (
	systemRBACMap     = make(map[string]AssetBlueprint)
	rbacRegistryMutex sync.RWMutex
)

// RegisterAssetRBAC ensures that new Assets declare their UI footprint and execution logic cleanly
func RegisterAssetRBAC(blueprint AssetBlueprint) {
	rbacRegistryMutex.Lock()
	defer rbacRegistryMutex.Unlock()
	systemRBACMap[blueprint.AssetType] = blueprint
}

// GetAssetBlueprint looks up the evaluator instructions by alias
func GetAssetBlueprint(assetType string) (AssetBlueprint, bool) {
	rbacRegistryMutex.RLock()
	defer rbacRegistryMutex.RUnlock()
	bp, found := systemRBACMap[assetType]
	return bp, found
}

// GetAllBlueprints returns a cloned map of the system-wide blueprints
func GetAllBlueprints() map[string]AssetBlueprint {
	rbacRegistryMutex.RLock()
	defer rbacRegistryMutex.RUnlock()

	result := make(map[string]AssetBlueprint, len(systemRBACMap))
	for k, v := range systemRBACMap {
		result[k] = v
	}
	return result
}
