package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

func TestMacro_AutoParameterize(t *testing.T) {
	// 1. Setup
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.NoCache,
	}
	sysDB := database.NewDatabase(dbOpts)
	ctx := context.Background()

	// Mock Generator Response
	mockResponse := `{
		"summary": "Auto Param Test Refined",
		"new_parameters": ["target_region", "min_salary"],
		"replacements": [
			{"value": "US", "parameter": "target_region", "description": "Replaced US"},
			{"value": "50000", "parameter": "min_salary", "description": "Replaced 50000"}
		]
	}`
	mockGen := &MockGenerator{Response: mockResponse}

	// Create Service
	svc := NewService(&MockDomain{}, sysDB, map[string]sop.DatabaseOptions{
		"system": dbOpts,
	}, mockGen, nil, nil, false)

	// Initialize session payload
	payload := &ai.SessionPayload{
		CurrentDB: "system",
	}
	ctx = context.WithValue(ctx, "session_payload", payload)

	// 2. Create Macro with hardcoded values
	svc.handleSessionCommand(ctx, "/macro create auto_test Auto Param Test", sysDB)

	// Add steps
	svc.session.LastStep = &ai.MacroStep{
		Type:    "command",
		Command: "select",
		Args:    map[string]any{"store": "users", "region": "US"},
	}
	svc.handleSessionCommand(ctx, "/macro step add auto_test bottom", sysDB)

	svc.session.LastStep = &ai.MacroStep{
		Type:    "command",
		Command: "select",
		Args:    map[string]any{"store": "employees", "salary": "50000"},
	}
	svc.handleSessionCommand(ctx, "/macro step add auto_test bottom", sysDB)

	// 3. Run Auto Parameterization (Refine)
	resp, handled, err := svc.handleSessionCommand(ctx, "/macro refine auto_test", sysDB)
	if err != nil {
		t.Fatalf("Auto parameterize (refine) failed: %v", err)
	}
	if !handled {
		t.Fatalf("Auto parameterize (refine) not handled")
	}

	// 4. Apply Refinement
	resp, handled, err = svc.handleSessionCommand(ctx, "/macro refine apply", sysDB)
	if err != nil {
		t.Fatalf("Refine apply failed: %v", err)
	}
	if !strings.Contains(resp, "updated successfully") {
		t.Errorf("Unexpected response from apply: %s", resp)
	}

	// 5. Verify Macro Content
	resp, _, _ = svc.handleSessionCommand(ctx, "/macro show auto_test", sysDB)
	if !strings.Contains(resp, "{{.target_region}}") {
		t.Errorf("Macro missing target_region replacement: %s", resp)
	}
	if !strings.Contains(resp, "{{.min_salary}}") {
		t.Errorf("Macro missing min_salary replacement: %s", resp)
	}
}
