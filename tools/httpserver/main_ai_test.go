package main

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
)

func TestInitializeRequest_UsesAuthenticatedUserID(t *testing.T) {
	req := httptest.NewRequest("POST", "/api/ai/chat", strings.NewReader(`{"message":"hi","user_id":"client-random"}`))
	req = req.WithContext(sop.ContextWithAuth(req.Context(), sop.AuthContext{UserID: "alice"}))

	chatReq, err := initializeRequest(nil, req)
	if err != nil {
		t.Fatalf("initializeRequest failed: %v", err)
	}
	if got := chatReq.UserID; got != "alice" {
		t.Fatalf("expected authenticated user ID to override request payload, got %q", got)
	}
}
