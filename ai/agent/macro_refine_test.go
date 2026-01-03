package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

func TestMacro_Refine(t *testing.T) {
	// 1. Setup
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.NoCache,
	}
	sysDB := database.NewDatabase(dbOpts)
	ctx := context.Background()

	// Mock Generator for Refine
	mockGen := &MockScriptedGenerator{
		Responses: []string{
			// Response for /macro refine
			`{
				"summary": "Fetches user data for a specific region.",
				"new_parameters": ["region"],
				"replacements": [
					{"value": "US", "parameter": "region", "description": "Replaced US with region"}
				]
			}`,
		},
	}

	// Create Service
	svc := NewService(&MockDomain{}, sysDB, map[string]sop.DatabaseOptions{
		"system": dbOpts,
	}, mockGen, nil, nil, false)

	// Initialize session payload
	payload := &ai.SessionPayload{
		CurrentDB: "system",
	}
	ctx = context.WithValue(ctx, "session_payload", payload)

	// 2. Create Macro with hardcoded value
	_, _, err := svc.handleSessionCommand(ctx, "/macro create refine_test Original Description", sysDB)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	svc.session.LastStep = &ai.MacroStep{
		Type:    "command",
		Command: "select",
		Args:    map[string]any{"store": "users", "region": "US"},
	}
	_, _, err = svc.handleSessionCommand(ctx, "/macro step add refine_test bottom", sysDB)
	if err != nil {
		t.Fatalf("Step add failed: %v", err)
	}

	// 3. Run /macro refine
	resp, handled, err := svc.handleSessionCommand(ctx, "/macro refine refine_test", sysDB)
	if err != nil {
		t.Fatalf("Refine failed: %v", err)
	}
	if !handled {
		t.Fatalf("Refine not handled")
	}
	if !strings.Contains(resp, "Refinement Proposal") {
		t.Errorf("Unexpected refine response: %s", resp)
	}
	if !strings.Contains(resp, "US -> {{.region}}") {
		t.Errorf("Refine response missing replacement info: %s", resp)
	}

	// 4. Run /macro refine apply
	resp, handled, err = svc.handleSessionCommand(ctx, "/macro refine apply", sysDB)
	if err != nil {
		t.Fatalf("Refine apply failed: %v", err)
	}
	if !strings.Contains(resp, "updated successfully") {
		t.Errorf("Unexpected apply response: %s", resp)
	}

	// 5. Verify Macro Changes (Summary & Params)
	resp, handled, err = svc.handleSessionCommand(ctx, "/macro show refine_test", sysDB)
	if err != nil {
		t.Fatalf("Show failed: %v", err)
	}
	if !strings.Contains(resp, "Parameters: [region]") {
		t.Errorf("Show output missing parameters: %s", resp)
	}
	if !strings.Contains(resp, "Description: Fetches user data") {
		t.Errorf("Show output missing new description: %s", resp)
	}

}
