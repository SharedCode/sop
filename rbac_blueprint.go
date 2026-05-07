package sop

import "context"

// UICapability represents the UI-friendly permission key (e.g., "can_edit", "can_delete")
type UICapability string

const (
	UICapabilityRead     UICapability = "can_read"
	UICapabilityEdit     UICapability = "can_edit"
	UICapabilityDelete   UICapability = "can_delete"
	UICapabilityAISelect UICapability = "can_ai_select"
)

// EndpointContext represents an API endpoint representing a grouping of assets.
type EndpointContext string

const (
	EndpointSpacesList EndpointContext = "/api/spaces"
	EndpointStoresList EndpointContext = "/api/stores"
	EndpointItemsList  EndpointContext = "/api/spaces/items"
)

// ContextRBACMap represents the UI-consumable map format representing capabilities for a given context: (Capability -> bool)
type ContextRBACMap map[UICapability]bool

// BundledResponse represents the standard JSON payload structure containing both the domain data and the paired RBAC map.
type BundledResponse struct {
	Data     interface{}               `json:"data"`
	RBAC     ContextRBACMap            `json:"rbac,omitempty"`
	ItemRBAC map[string]ContextRBACMap `json:"item_rbac,omitempty"`
}

// ActionToUICapability maps an internal Action to the UI consumable Capability string
func ActionToUICapability(action Action) UICapability {
	switch action {
	case ActionRead:
		return UICapabilityRead
	case ActionWrite:
		return UICapabilityEdit
	case ActionDelete:
		return UICapabilityDelete
	case ActionAISelect:
		return UICapabilityAISelect
	default:
		return UICapability(string(action))
	}
}

// EntitlementContext holds the request scope parameters necessary for granular RBAC evaluation.
type EntitlementContext struct {
	AssetID    string
	Database   string
	IsSystemDB bool
	UserRole   string
}

// ResolveRBACMap evaluates the dynamic AssetBlueprint for a functional context (e.g., current space/store), delegating to the core evaluator.
func ResolveRBACMap(ctx context.Context, assetType string, entitlementCtx EntitlementContext, getLocalAccess func() ResourceAccess) ContextRBACMap {
	bp, exists := GetAssetBlueprint(assetType)
	if !exists {
		return make(ContextRBACMap) // Safety fallback
	}

	capabilities := make(ContextRBACMap)

	var localAccess ResourceAccess
	if getLocalAccess != nil {
		localAccess = getLocalAccess()
	}

	for _, action := range bp.Actions {
		uiCap := ActionToUICapability(action)
		if bp.Evaluator != nil {
			capabilities[uiCap] = bp.Evaluator(ctx, entitlementCtx, action)
		} else {
			capabilities[uiCap] = CanPerformAction(ctx, entitlementCtx.AssetID, localAccess, action)
		}
	}

	return capabilities
}
