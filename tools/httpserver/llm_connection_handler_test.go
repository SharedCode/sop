package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleTestLLMConnection_MethodNotAllowed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/ai/test-connection", nil)
	w := httptest.NewRecorder()

	handleTestLLMConnection(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestHandleTestLLMConnection_InvalidProvider(t *testing.T) {
	body := []byte(`{"provider":"not-a-provider","model":"x"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/ai/test-connection", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handleTestLLMConnection(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, w.Code)
	}
}
