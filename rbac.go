package sop

import (
	"context"
	"errors"
)

var (
	ErrSystemReadOnly = errors.New("system knowledge bases are read-only")
	ErrQuotaExceeded  = errors.New("quota exceeded")
	ErrUnauthorized   = errors.New("unauthorized access")
)

type Visibility string

const (
	VisibilityPublic  Visibility = "public"
	VisibilityPrivate Visibility = "private"
	VisibilitySystem  Visibility = "system"
)

const (
	RoleAdmin = "Admin"
	RoleUser  = "User"
	RoleGuest = "Guest"
)

type ResourceAccess struct {
	Visibility Visibility          `json:"visibility"`
	OwnerID    string              `json:"owner_id,omitempty"`
	Roles      map[string][]string `json:"roles,omitempty"`
	Users      map[string][]string `json:"users,omitempty"`
}

type Action string

const (
	ActionRead   Action = "read"
	ActionWrite  Action = "write"
	ActionDelete Action = "delete"
	ActionList   Action = "list"
)

type AuthContext struct {
	UserID   string
	Roles    []string
	IsSystem bool
}

type authContextKey string

const authKey authContextKey = "sop_auth"

func ContextWithAuth(ctx context.Context, auth AuthContext) context.Context {
	return context.WithValue(ctx, authKey, auth)
}

func GetAuthFromContext(ctx context.Context) AuthContext {
	if auth, ok := ctx.Value(authKey).(AuthContext); ok {
		return auth
	}
	return AuthContext{}
}

func Authorize(ctx context.Context, access ResourceAccess, action Action) bool {
	caller := GetAuthFromContext(ctx)

	if access.Visibility == VisibilitySystem {
		return caller.IsSystem
	}

	for _, role := range caller.Roles {
		if role == RoleAdmin {
			return true
		}
	}

	if access.OwnerID != "" && caller.UserID == access.OwnerID {
		return true
	}

	if access.Visibility == VisibilityPublic || access.Visibility == "" {
		if action == ActionRead || action == ActionList {
			return true
		}
	}

	for _, role := range caller.Roles {
		if allowedActions, ok := access.Roles[role]; ok {
			for _, a := range allowedActions {
				if a == string(action) || a == "*" {
					return true
				}
			}
		}
	}

	if allowedActions, ok := access.Users[caller.UserID]; ok {
		for _, a := range allowedActions {
			if a == string(action) || a == "*" {
				return true
			}
		}
	}

	return false
}

// IsSystemReadOnly returns true if the specified resource is a core system resource
// that must remain read-only to prevent destructive actions by any user.
func IsSystemReadOnly(resourceName string) bool {
	return resourceName == "SOP" || resourceName == "LongTermMemory"
}

// CheckPolicy evaluates the three-layer RBAC model and returns an error if access is denied.
// It is useful when you need to know exactly *why* access was denied.
func CheckPolicy(ctx context.Context, resourceName string, access ResourceAccess, action Action) error {
	// Layer 1: System Invariants
	if IsSystemReadOnly(resourceName) {
		if action == ActionWrite || action == ActionDelete {
			return ErrSystemReadOnly
		}
	}

	// Layer 2: Global Entitlements

	// TODO: Fetch caller identity, load their Roles from system_roles, and apply Quota checks.
	// Example: Verify max Spaces limit is not exceeded here.

	// Layer 3: Local Resource ACL
	if !Authorize(ctx, access, action) {
		return ErrUnauthorized
	}

	return nil
}

// EnforcePolicy checks the policy and returns an error if the action is not allowed.
// Use CanPerformAction for a boolean result (e.g. for adjusting UI states).
func EnforcePolicy(ctx context.Context, resourceName string, access ResourceAccess, action Action) error {
	return CheckPolicy(ctx, resourceName, access, action)
}

// CanPerformAction checks if the current context has permission to perform the action on the resource.
// It returns a boolean, making it ideal for UI visibility toggles like IsReadOnly.
func CanPerformAction(ctx context.Context, resourceName string, access ResourceAccess, action Action) bool {
	return CheckPolicy(ctx, resourceName, access, action) == nil
}
