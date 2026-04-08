package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/sharedcode/sop/ai"
)

// mockAgent for testing cloning and session handling
type mockAgent struct {
	id string
}

func (m *mockAgent) Open(ctx context.Context) error {
	return nil
}
func (m *mockAgent) Close(ctx context.Context) error {
	return nil
}
func (m *mockAgent) Search(ctx context.Context, query string, limit int) ([]ai.Hit[map[string]any], error) {
	return nil, nil
}
func (m *mockAgent) Ask(ctx context.Context, query string, opts ...ai.Option) (string, error) {
	return "Mock Response from " + m.id, nil
}

func (m *mockAgent) Clone() ai.Agent[map[string]any] {
	return &mockAgent{id: "cloned"}
}

func TestSessionIDHandling(t *testing.T) {
	// Setup
	activeSessions = NewSessionManager(100)

	loadedAgents = map[string]ai.Agent[map[string]any]{
		"default": &mockAgent{id: "blueprint"},
	}

	// Request 1: No SessionID -> Expect a new Session ID to be generated
	reqBody := `{"message": "Hello", "database": "", "store": "", "agent": "default"}`
	req := httptest.NewRequest("POST", "/ai/chat", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	handleAIChat(w, req)

	res := w.Result()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("Expected status OK, got %v", res.Status)
	}

	body := w.Body.String()
	if !strings.Contains(body, "session_id") {
		t.Fatalf("Expected response to emit a session_id event, got: %s", body)
	}

	// Extract session ID from simple NDJSON parsing
	var sessionID string
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if strings.Contains(line, `"type":"session_id"`) || strings.Contains(line, `"event":"session_id"`) {
			var event map[string]any
			if err := json.Unmarshal([]byte(line), &event); err == nil {
				if d, ok := event["payload"].(string); ok {
					sessionID = d
					break
				}
			}
		}
	}

	if sessionID == "" {
		t.Fatal("Failed to parse session_id from response")
	}

	// Verify the session agent was stored

	sessionAgent, exists := activeSessions.Get(sessionID)

	if !exists {
		t.Fatal("Expected agent to be stored in activeSessions map, but it was missing")
	}

	// Verify it was cloned
	if sa, ok := sessionAgent.(*mockAgent); !ok || sa.id != "cloned" {
		t.Fatalf("Expected agent to be 'cloned', got %v", sessionAgent)
	}

	// Request 2: Use the existing session ID
	reqBody2 := `{"message": "Hello again", "database": "", "store": "", "agent": "default", "session_id": "` + sessionID + `"}`
	req2 := httptest.NewRequest("POST", "/ai/chat", bytes.NewBufferString(reqBody2))
	req2.Header.Set("Content-Type", "application/json")

	w2 := httptest.NewRecorder()
	handleAIChat(w2, req2)

	// Verify the map still contains exactly 1 item

	activeSessions.mu.Lock()
	count := len(activeSessions.lookup)
	activeSessions.mu.Unlock()

	if count != 1 {
		t.Fatalf("Expected strictly 1 session, but activeSessions grew to %d", count)
	}
}

func TestSessionIDConcurrency(t *testing.T) {
	// Setup
	activeSessions = NewSessionManager(100)

	loadedAgents = map[string]ai.Agent[map[string]any]{
		"default": &mockAgent{id: "blueprint"},
	}

	var wg sync.WaitGroup
	numRequests := 50

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reqBody := `{"message": "Hello", "database": "", "store": "", "agent": "default"}`
			req := httptest.NewRequest("POST", "/ai/chat", bytes.NewBufferString(reqBody))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			handleAIChat(w, req)

			if w.Result().StatusCode != http.StatusOK {
				t.Errorf("Unexpected status code!")
			}
		}()
	}

	wg.Wait()

	activeSessions.mu.Lock()
	count := len(activeSessions.lookup)
	activeSessions.mu.Unlock()

	if count != numRequests {
		t.Fatalf("Expected %d active sessions after concurrent requests, got %d", numRequests, count)
	}
}
