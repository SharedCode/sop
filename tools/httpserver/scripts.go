package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent"
)

// ExecuteScriptRequest defines the payload for executing a script.
type ExecuteScriptRequest struct {
	Name     string         `json:"name"`
	Category string         `json:"category"`
	Args     map[string]any `json:"args"`
	Agent    string         `json:"agent"` // Optional: specify which agent to use
}

// handleExecuteScript handles the POST /api/scripts/execute endpoint.
func handleExecuteScript(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ExecuteScriptRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON body: %v", err), http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		http.Error(w, "Script name is required", http.StatusBadRequest)
		return
	}

	if req.Category == "" {
		req.Category = "general"
	}

	// Determine which agent to use
	agentID := req.Agent
	if agentID == "" {
		agentID = "sql_admin" // Default to sql_admin as it's the main one loaded
	}

	agentIntf, exists := loadedAgents[agentID]
	if !exists {
		// Fallback: try to find any loaded agent if the default one isn't there
		if len(loadedAgents) > 0 {
			for k, v := range loadedAgents {
				agentID = k
				agentIntf = v
				break
			}
		} else {
			http.Error(w, fmt.Sprintf("Agent '%s' not found and no agents loaded", agentID), http.StatusInternalServerError)
			return
		}
	}

	// Type assert to *agent.Service to access PlayScript
	// Note: loadedAgents is map[string]ai.Agent[map[string]any]
	// We need to check if the underlying implementation is *agent.Service
	agentSvc, ok := agentIntf.(*agent.Service)
	if !ok {
		http.Error(w, fmt.Sprintf("Agent '%s' does not support script execution (not an *agent.Service)", agentID), http.StatusInternalServerError)
		return
	}

	// Create context with session payload
	ctx := r.Context()
	payload := &ai.SessionPayload{
		CurrentDB: "system", // Default to system or make configurable
	}
	ctx = context.WithValue(ctx, "session_payload", payload)

	// Inject ScriptRecorder into context so executed steps are recorded in session state
	// This enables /last-tool to work after running a script.
	ctx = context.WithValue(ctx, ai.CtxKeyScriptRecorder, agentSvc)

	// Set headers for streaming
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Transfer-Encoding", "chunked")
	// We don't strictly need to set status 200 here, the first Write will do it.
	// But it's good to be explicit if we haven't encountered an error yet.

	// Use PlayScript with the response writer
	// PlayScript writes JSON chunks to w
	if err := agentSvc.PlayScript(ctx, req.Name, req.Category, req.Args, w); err != nil {
		// If an error occurs during streaming, we can't easily change the HTTP status code
		// if headers have already been sent (which happens on the first Write).
		// However, PlayScript is expected to write error details to the stream if possible,
		// or we can log it here.
		// Since we are streaming JSON, appending a JSON error object might be invalid if the stream is mid-object.
		// For now, we just log it.
		fmt.Printf("Error executing script '%s': %v\n", req.Name, err)
	}
}

// withAuth is a middleware that optionally enforces Bearer token authentication.
func withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// If Auth is disabled, skip checks
		if !config.EnableRestAuth {
			next(w, r)
			return
		}

		// If Auth is enabled but no password is set, we fail closed (secure by default)
		if config.RootPassword == "" {
			http.Error(w, "Server configuration error: Auth enabled but no RootPassword set", http.StatusInternalServerError)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Unauthorized: Missing Authorization header", http.StatusUnauthorized)
			return
		}

		// Check for "Bearer <token>"
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			http.Error(w, "Unauthorized: Invalid Authorization header format", http.StatusUnauthorized)
			return
		}

		token := parts[1]
		if token != config.RootPassword {
			http.Error(w, "Unauthorized: Invalid token", http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}
