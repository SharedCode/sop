package sop

import (
	"context"
	"testing"
)

func TestAssetBlueprintRegistry(t *testing.T) {
	// Re-initialize for isolated test
	rbacRegistryMutex.Lock()
	systemRBACMap = make(map[string]AssetBlueprint)
	rbacRegistryMutex.Unlock()

	blueprint := AssetBlueprint{
		AssetType:   "test_asset",
		Description: "A test asset for unit tests",
		Endpoints:   []string{"/api/test_assets"},
		Actions:     []Action{ActionRead, ActionWrite},
		Evaluator: func(ctx context.Context, entitlementCtx EntitlementContext, action Action) bool {
			return true
		},
	}
	RegisterAssetRBAC(blueprint)
	bp, found := GetAssetBlueprint("test_asset")
	if !found {
		t.Fatalf("Expected to find test_asset in global registry but did not")
	}

	if bp.AssetType != "test_asset" || bp.Description != "A test asset for unit tests" {
		t.Errorf("Retrieved blueprint does not match registered blueprint")
	}

	all := GetAllBlueprints()
	if len(all) != 1 {
		t.Errorf("Expected 1 blueprint, got %d", len(all))
	}
	if _, exists := all["test_asset"]; !exists {
		t.Errorf("Expected test_asset in returned global blueprints")
	}
}
