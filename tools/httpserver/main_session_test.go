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
func (m *mockAgent) Ask(ctx context.Context, query string, cfg *ai.ConfigMap) (string, error) {
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

// payloadMockAgent for testing SessionPayload extraction
type payloadMockAgent struct {
	id          string
	capturedCfg *ai.ConfigMap
}

func (m *payloadMockAgent) Open(ctx context.Context) error  { return nil }
func (m *payloadMockAgent) Close(ctx context.Context) error { return nil }
func (m *payloadMockAgent) Search(ctx context.Context, query string, limit int) ([]ai.Hit[map[string]any], error) {
	return nil, nil
}
func (m *payloadMockAgent) Ask(ctx context.Context, query string, cfg *ai.ConfigMap) (string, error) {
	m.capturedCfg = cfg
	return "Payload Mock Response", nil
}
func (m *payloadMockAgent) Clone() ai.Agent[map[string]any] {
	return &payloadMockAgent{id: "cloned"}
}

func TestSessionPayloadPropagation(t *testing.T) {
	// Setup
	activeSessions = NewSessionManager(100)
	mock := &payloadMockAgent{id: "blueprint"}
	loadedAgents = map[string]ai.Agent[map[string]any]{
		"payload_tester": mock,
	}

	// Test passing SelectedKBs and Domain
	reqBody := `{"message": "Hello", "domain": "Finance", "selected_kbs": [{"name": "sys:SOP"}, {"name": "curr:Finance"}], "agent": "payload_tester"}`
	req := httptest.NewRequest("POST", "/ai/chat", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	handleAIChat(w, req)

	res := w.Result()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("Expected status OK, got %v", res.Status)
	}

	// Parse response to find session ID so we can get the cloned agent
	body := w.Body.String()
	var sessionID string
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if strings.Contains(line, `"type":"session_id"`) {
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

	agentRaw, exists := activeSessions.Get(sessionID)
	if !exists {
		t.Fatal("Agent not found in session manager")
	}

	pmAgent, ok := agentRaw.(*payloadMockAgent)
	if !ok {
		t.Fatal("Agent is not a payloadMockAgent")
	}

	// Check if captured config has the SessionPayload
	var payload *ai.SessionPayload
	if pmAgent.capturedCfg != nil {
		if p, ok := pmAgent.capturedCfg.Get("payload"); ok {
			if sp, ok := p.(*ai.SessionPayload); ok {
				payload = sp
			}
		}
	}

	if payload == nil {
		t.Fatal("Expected SessionPayload to be populated in options")
	}
	if payload.ActiveDomain != "Finance" {
		t.Errorf("Expected ActiveDomain 'Finance', got '%s'", payload.ActiveDomain)
	}
	if len(payload.SelectedKBs) != 2 {
		t.Fatalf("Expected 2 SelectedKBs, got %d", len(payload.SelectedKBs))
	}
	if payload.SelectedKBs[0].Name != "sys:SOP" || payload.SelectedKBs[1].Name != "curr:Finance" {
		t.Errorf("SelectedKBs mismatch: %v", payload.SelectedKBs)
	}
}

type eventStreamingMockAgent struct{}

func (m *eventStreamingMockAgent) Ask(ctx context.Context, query string, cfg *ai.ConfigMap) (string, error) {
	if streamer, ok := ctx.Value(ai.CtxKeyEventStreamer).(func(string, any)); ok && streamer != nil {
		streamer("tool_call", map[string]any{
			"tool": "mint_to_space",
			"args": map[string]any{"kb_name": "Tasks2"},
		})
		streamer("tool_result", map[string]any{
			"tool":   "mint_to_space",
			"args":   map[string]any{"kb_name": "Tasks2"},
			"result": "ok",
		})
	}
	return "Plain final answer.", nil
}

func (m *eventStreamingMockAgent) Open(ctx context.Context) error  { return nil }
func (m *eventStreamingMockAgent) Close(ctx context.Context) error { return nil }
func (m *eventStreamingMockAgent) Search(ctx context.Context, query string, limit int) ([]ai.Hit[map[string]any], error) {
	return nil, nil
}
func (m *eventStreamingMockAgent) Clone() ai.Agent[map[string]any] { return &eventStreamingMockAgent{} }

func TestSetupStream_EmitsVisibleContentForToolResults(t *testing.T) {
	w := httptest.NewRecorder()
	sendEvent, _ := setupStream(w)

	sendEvent(ai.ReasoningEventToolResult, map[string]any{
		"tool":   "lookup_user",
		"result": "John is in the users store.",
	})

	body := w.Body.String()
	if !strings.Contains(body, `"type":"content"`) {
		t.Fatalf("expected content event for tool_result, got: %s", body)
	}
	if !strings.Contains(body, "Result from lookup_user") {
		t.Fatalf("expected human-readable tool result content, got: %s", body)
	}
}

func TestSetupStream_HandlesListStoresResultAsContent(t *testing.T) {
	w := httptest.NewRecorder()
	sendEvent, _ := setupStream(w)

	sendEvent(ai.ReasoningEventToolResult, map[string]any{
		"tool":   "list_stores",
		"result": `[{"name":"alpha"}]`,
	})

	body := w.Body.String()
	if !strings.Contains(body, `"type":"content"`) {
		t.Fatalf("expected content event for list_stores result, got: %s", body)
	}
	if !strings.Contains(body, "list_stores") {
		t.Fatalf("expected list_stores result content in stream, got: %s", body)
	}
}

func TestSetupStream_UnescapesToolResultText(t *testing.T) {
	if got := renderVisibleText(`Link table: UserID -\u003e OrderID`); got != "Link table: UserID -> OrderID" {
		t.Fatalf("expected real arrow rendering for escaped unicode text, got: %q", got)
	}
}

func TestHandleAIChat_StreamsStructuredToolEvents(t *testing.T) {
	activeSessions = NewSessionManager(100)
	loadedAgents = map[string]ai.Agent[map[string]any]{
		"event_tester": &eventStreamingMockAgent{},
	}

	reqBody := `{"message": "Create tasks", "database": "", "agent": "event_tester"}`
	req := httptest.NewRequest("POST", "/ai/chat", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	handleAIChat(w, req)

	res := w.Result()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("Expected status OK, got %v", res.Status)
	}

	body := w.Body.String()
	if !strings.Contains(body, `"type":"tool_call"`) {
		t.Fatalf("expected tool_call event in NDJSON stream, got: %s", body)
	}
	if !strings.Contains(body, `"type":"tool_result"`) {
		t.Fatalf("expected tool_result event in NDJSON stream, got: %s", body)
	}
	if !strings.Contains(body, `"kb_name":"Tasks2"`) {
		t.Fatalf("expected structured tool args in NDJSON stream, got: %s", body)
	}
}
