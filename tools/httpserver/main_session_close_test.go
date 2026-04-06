package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleCloseSession(t *testing.T) {
	// Setup
	activeSessions = NewSessionManager(100)
	activeSessions.Put("test-session-123", &mockAgent{id: "test"})

	// Happy Path
	req := httptest.NewRequest("DELETE", "/api/ai/session/close?session_id=test-session-123", nil)
	w := httptest.NewRecorder()

	handleCloseSession(w, req)

	res := w.Result()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("Expected status OK, got %v", res.Status)
	}

	// Verify session is closed
	if _, exists := activeSessions.Get("test-session-123"); exists {
		t.Fatal("Expected session to be closed and removed")
	}

	// Test missing session_id param
	reqMissing := httptest.NewRequest("DELETE", "/api/ai/session/close", nil)
	wMissing := httptest.NewRecorder()
	handleCloseSession(wMissing, reqMissing)
	if wMissing.Result().StatusCode != http.StatusBadRequest {
		t.Fatalf("Expected Bad Request for missing session_id, got %v", wMissing.Result().StatusCode)
	}

	// Test wrong method
	reqWrongMethod := httptest.NewRequest("GET", "/api/ai/session/close?session_id=foo", nil)
	wWrongMethod := httptest.NewRecorder()
	handleCloseSession(wWrongMethod, reqWrongMethod)
	if wWrongMethod.Result().StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("Expected Method Not Allowed, got %v", wWrongMethod.Result().StatusCode)
	}
}
