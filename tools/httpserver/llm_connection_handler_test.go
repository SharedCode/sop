package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAIConnectionTestTimeout_Default(t *testing.T) {
	t.Setenv("SOP_AI_CONNECTION_TEST_TIMEOUT", "")

	if got := aiConnectionTestTimeout(); got != defaultAIConnectionTestTimeout {
		t.Fatalf("expected default timeout %v, got %v", defaultAIConnectionTestTimeout, got)
	}
}

func TestAIConnectionTestTimeout_UsesEnvOverride(t *testing.T) {
	t.Setenv("SOP_AI_CONNECTION_TEST_TIMEOUT", "90s")

	if got := aiConnectionTestTimeout(); got != 90*time.Second {
		t.Fatalf("expected env override timeout %v, got %v", 90*time.Second, got)
	}
}

func TestAIConnectionTestTimeout_InvalidEnvFallsBack(t *testing.T) {
	t.Setenv("SOP_AI_CONNECTION_TEST_TIMEOUT", "bad-timeout")

	if got := aiConnectionTestTimeout(); got != defaultAIConnectionTestTimeout {
		t.Fatalf("expected fallback timeout %v, got %v", defaultAIConnectionTestTimeout, got)
	}
}

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

func TestHandleTestEmbedderConnection_MethodNotAllowed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/ai/test-embedder-connection", nil)
	w := httptest.NewRecorder()

	handleTestEmbedderConnection(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestHandleTestEmbedderConnection_InvalidProvider(t *testing.T) {
	body := []byte(`{"provider":"not-a-provider","model":"text-embedding-3-small"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/ai/test-embedder-connection", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handleTestEmbedderConnection(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestHandleTestEmbedderConnection_OpenAIUsesConfiguredAPIURL(t *testing.T) {
	var capturedPath string
	var capturedAuth string
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{
				"embedding": []float64{0.1, 0.2, 0.3},
			}},
		})
	}))
	defer relay.Close()

	body := []byte(`{"provider":"openai","model":"text-embedding-3-small","api_key":"anything","url":"` + relay.URL + `/v1"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/ai/test-embedder-connection", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handleTestEmbedderConnection(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d with body %s", http.StatusOK, w.Code, w.Body.String())
	}
	if capturedPath != "/v1/embeddings" {
		t.Fatalf("expected relay embeddings path, got %q", capturedPath)
	}
	if capturedAuth != "Bearer anything" {
		t.Fatalf("expected bearer auth header, got %q", capturedAuth)
	}
}

func TestHandleTestLLMConnection_OpenAIUsesConfiguredAPIURL(t *testing.T) {
	var capturedPath string
	var capturedAuth string
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message":       map[string]any{"content": "OK"},
				"finish_reason": "stop",
			}},
			"usage": map[string]any{"total_tokens": 1},
		})
	}))
	defer relay.Close()

	body := []byte(`{"provider":"openai","model":"gpt-5.4","api_key":"anything","url":"` + relay.URL + `/v1"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/ai/test-connection", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handleTestLLMConnection(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d with body %s", http.StatusOK, w.Code, w.Body.String())
	}
	if capturedPath != "/v1/chat/completions" {
		t.Fatalf("expected relay responses path, got %q", capturedPath)
	}
	if capturedAuth != "Bearer anything" {
		t.Fatalf("expected bearer auth header, got %q", capturedAuth)
	}
}
