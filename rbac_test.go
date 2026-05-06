package sop

import (
	"context"
	"testing"
)

func TestContextWithAuth(t *testing.T) {
	ctx := context.Background()
	auth := AuthContext{
		UserID:   "user123",
		Roles:    []string{"admin"},
		IsSystem: true,
	}

	newCtx := ContextWithAuth(ctx, auth)
	gotAuth := GetAuthFromContext(newCtx)

	if gotAuth.UserID != auth.UserID {
		t.Errorf("expected UserID %s, got %s", auth.UserID, gotAuth.UserID)
	}
	if gotAuth.IsSystem != auth.IsSystem {
		t.Errorf("expected IsSystem %v, got %v", auth.IsSystem, gotAuth.IsSystem)
	}
	if len(gotAuth.Roles) != 1 || gotAuth.Roles[0] != auth.Roles[0] {
		t.Errorf("expected Roles %v, got %v", auth.Roles, gotAuth.Roles)
	}
}

func TestGetAuthFromContext_NotFound(t *testing.T) {
	ctx := context.Background()
	auth := GetAuthFromContext(ctx)

	if auth.UserID != "" {
		t.Errorf("expected empty UserID, got %s", auth.UserID)
	}
}

func TestAuthorize_SystemVisibility(t *testing.T) {
	tests := []struct {
		name     string
		isSystem bool
		expected bool
	}{
		{"system user", true, true},
		{"regular user", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := ContextWithAuth(context.Background(), AuthContext{IsSystem: tt.isSystem})
			access := ResourceAccess{Visibility: VisibilitySystem}

			if got := Authorize(ctx, access, ActionRead); got != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, got)
			}
		})
	}
}

func TestAuthorize_Owner(t *testing.T) {
	ctx := ContextWithAuth(context.Background(), AuthContext{UserID: "owner1"})
	access := ResourceAccess{
		Visibility: VisibilityPrivate,
		OwnerID:    "owner1",
	}

	if got := Authorize(ctx, access, ActionWrite); !got {
		t.Errorf("owner should be authorized")
	}

	// different user
	ctx2 := ContextWithAuth(context.Background(), AuthContext{UserID: "other"})
	if got := Authorize(ctx2, access, ActionRead); got {
		t.Errorf("non-owner shouldn't be authorized on private without roles")
	}
}

func TestAuthorize_PublicVisibility(t *testing.T) {
	ctx := ContextWithAuth(context.Background(), AuthContext{UserID: "other"})
	access := ResourceAccess{Visibility: VisibilityPublic}
	accessEmpty := ResourceAccess{} // empty should act like public for read/list

	if got := Authorize(ctx, access, ActionRead); !got {
		t.Errorf("public should allow read")
	}
	if got := Authorize(ctx, access, ActionList); !got {
		t.Errorf("public should allow list")
	}
	if got := Authorize(ctx, access, ActionWrite); got {
		t.Errorf("public shouldn't allow write")
	}
	if got := Authorize(ctx, accessEmpty, ActionRead); !got {
		t.Errorf("empty visibility should allow read")
	}
}

func TestAuthorize_Roles(t *testing.T) {
	ctx := ContextWithAuth(context.Background(), AuthContext{Roles: []string{"editor", "viewer"}})
	access := ResourceAccess{
		Visibility: VisibilityPrivate,
		Roles: map[string][]string{
			"editor": {"write", "delete"},
			"reader": {"*"},
		},
	}

	if got := Authorize(ctx, access, ActionWrite); !got {
		t.Errorf("editor role should allow write")
	}
	if got := Authorize(ctx, access, ActionDelete); !got {
		t.Errorf("editor role should allow delete")
	}
	if got := Authorize(ctx, access, ActionRead); got {
		t.Errorf("editor role shouldn't allow read (not granted)")
	}

	ctxReader := ContextWithAuth(context.Background(), AuthContext{Roles: []string{"reader"}})
	if got := Authorize(ctxReader, access, ActionDelete); !got {
		t.Errorf("reader role with * should allow delete")
	}
}

func TestAuthorize_Users(t *testing.T) {
	ctx := ContextWithAuth(context.Background(), AuthContext{UserID: "user123"})
	access := ResourceAccess{
		Visibility: VisibilityPrivate,
		Users: map[string][]string{
			"user123": {"read", "list"},
			"user999": {"*"},
		},
	}

	if got := Authorize(ctx, access, ActionRead); !got {
		t.Errorf("user specific rule should allow read")
	}
	if got := Authorize(ctx, access, ActionWrite); got {
		t.Errorf("user specific rule shouldn't allow write")
	}

	ctxAll := ContextWithAuth(context.Background(), AuthContext{UserID: "user999"})
	if got := Authorize(ctxAll, access, ActionDelete); !got {
		t.Errorf("user with * should allow delete")
	}
}
