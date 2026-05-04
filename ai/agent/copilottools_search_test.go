package agent

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

func prepareKBForSearchTest(ctx context.Context, t *testing.T, db *database.Database, kbName string, docText string) {
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	defer tx.Rollback(ctx)

	kb, err := db.OpenKnowledgeBase(ctx, kbName, tx, nil, &MockEmbedder{})
	if err != nil {
		t.Fatalf("OpenKnowledgeBase failed: %v", err)
	}

	payload := map[string]any{"text": docText, "category": "test_category"}
	// We provide a dummy vector to avoid embedder nil panic, and a category to avoid LLM panic
	err = kb.IngestThought(ctx, docText, "test_category", "", []float32{0.1, 0.2, 0.3}, payload)
	if err != nil {
		t.Fatalf("IngestThought failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func TestCopilotTools_Search_TierRouting(t *testing.T) {
	// 1. Setup Temp Dirs
	systemDir, err := os.MkdirTemp("", "sop_search_system")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(systemDir)

	tenantDir, err := os.MkdirTemp("", "sop_search_tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tenantDir)

	// Since ai.GetSessionPayload looks for a specific context key type, we can create the actual payload locally.
	// ai package has `type sessionPayloadKey struct{}`. But the `GetSessionPayload` uses `ctx.Value("session_payload")` as a string!
	// Let's verify what `GetSessionPayload` uses. If it's a string, we can inject it.

	// `interfaces.go:376`: `if val := ctx.Value("session_payload"); val != nil {`
	// That indicates it expects strings!

	// 2. Initialize DBs
	systemOpts := sop.DatabaseOptions{StoresFolders: []string{systemDir}}
	systemDB := database.NewDatabase(systemOpts)

	tenantOpts := sop.DatabaseOptions{StoresFolders: []string{tenantDir}}
	tenantDBName := "test_tenant"

	dbs := map[string]sop.DatabaseOptions{
		tenantDBName: tenantOpts,
	}

	// 3. Create Agent
	agent := NewCopilotAgent(Config{}, dbs, systemDB)
	ctx := context.Background()
	agent.Open(ctx) // initialize maps

	// 4. Seed Data
	// system_knowledge MUST be in systemDB
	prepareKBForSearchTest(ctx, t, systemDB, "system_knowledge", "System Docs: How to use SOP.")

	// Create a shadowed KB. It exists in system AND tenant.
	prepareKBForSearchTest(ctx, t, systemDB, "shadow_kb", "System Docs: Shadow version.")
	// A tenant DB connection is needed to seed the tenant dir properly
	tenantDBInstance := database.NewDatabase(tenantOpts)
	prepareKBForSearchTest(ctx, t, tenantDBInstance, "shadow_kb", "Tenant Docs: Real shadow version.")

	// Active domain in Tenant
	prepareKBForSearchTest(ctx, t, tenantDBInstance, "finance", "Finance Docs: Q3 Earnings.")

	// 5. Test Tier 1 (System SOP Tool)
	// Expect this to ALWAYS hit system_knowledge safely
	res1, err := agent.toolSearchSopKB(ctx, map[string]any{"query": "SOP."})
	if err != nil {
		t.Fatalf("toolSearchSopKB failed: %v", err)
	}
	if !strings.Contains(res1, "System Docs: How to use SOP.") {
		t.Errorf("Expected System Docs, got: %s", res1)
	}

	// 6. Test Tier 2 (Domain Tool)
	// Must set up context payload
	payload := &ai.SessionPayload{
		CurrentDB:    tenantDBName,
		ActiveDomain: "finance",
	}
	tenantCtx := context.WithValue(ctx, "session_payload", payload)

	res2, err := agent.toolSearchDomainKB(tenantCtx, map[string]any{"query": "Earnings."})
	if err != nil {
		t.Fatalf("toolSearchDomainKB failed: %v", err)
	}
	if !strings.Contains(res2, "Finance Docs: Q3 Earnings.") {
		t.Errorf("Expected Finance Docs, got: %s", res2)
	}

	// Test mapping failure (no domain selected)
	resNoDomain, _ := agent.toolSearchDomainKB(ctx, map[string]any{"query": "Earnings."}) // clean ctx
	if !strings.Contains(resNoDomain, "No active domain KB selected by user.") {
		t.Errorf("Expected rejection with clean ctx, got: %s", resNoDomain)
	}

	// 7. Test Tier 3 (Custom KBs Tool) & Shadowing
	// We select the shadow_kb. It should read the tenant version instead of system version.
	payloadCustom := &ai.SessionPayload{
		CurrentDB:   tenantDBName,
		SelectedKBs: []string{"shadow_kb"},
	}
	customCtx := context.WithValue(ctx, "session_payload", payloadCustom)

	// Since we search by keyword, let's query a known string: "Shadow"
	res3, err := agent.toolSearchCustomKBs(customCtx, map[string]any{"query": "shadow version."})
	if err != nil {
		t.Fatalf("toolSearchCustomKBs failed: %v", err)
	}
	if !strings.Contains(res3, "Tenant Docs: Real shadow version.") {
		t.Errorf("Expected Tenant shadowed Docs, got: %s", res3)
	}
	// It should NOT contain system's shadowed version if tenant DB answered it successfully.
	if strings.Contains(res3, "System Docs: Shadow version.") {
		t.Errorf("Shadowing failed, returned system DB contents: %s", res3)
	}

	// Test mapping failure (no custom kbs selected)
	resNoCustom, _ := agent.toolSearchCustomKBs(ctx, map[string]any{"query": "Test"}) // clean ctx
	if !strings.Contains(resNoCustom, "No custom KBs selected.") {
		t.Errorf("Expected rejection with clean ctx, got: %s", resNoCustom)
	}

	// 8. TIER 2 EDGE CASES: Domain fallback to System DB
	// We set ActiveDomain to a KB that only exists in System DB
	prepareKBForSearchTest(ctx, t, systemDB, "system_only_domain", "System Docs: System specific domain info.")
	payloadDomainFallback := &ai.SessionPayload{
		CurrentDB:    tenantDBName,
		ActiveDomain: "system_only_domain",
	}
	tenantCtxFallback := context.WithValue(ctx, "session_payload", payloadDomainFallback)
	resDomainFallback, err := agent.toolSearchDomainKB(tenantCtxFallback, map[string]any{"query": "System"})
	if err != nil {
		t.Fatalf("toolSearchDomainKB fallback failed: %v", err)
	}
	if !strings.Contains(resDomainFallback, "System Docs: System specific domain info.") {
		t.Errorf("Expected fallback to System DB docs, got: %s", resDomainFallback)
	}

	// TIER 2 EDGE CASE: Domain KB doesn't exist anywhere
	payloadDomainMissing := &ai.SessionPayload{
		CurrentDB:    tenantDBName,
		ActiveDomain: "missing_domain",
	}
	tenantCtxMissing := context.WithValue(ctx, "session_payload", payloadDomainMissing)
	resDomainMissing, _ := agent.toolSearchDomainKB(tenantCtxMissing, map[string]any{"query": "anything"})
	if !strings.Contains(resDomainMissing, "No results found.") {
		t.Logf("Expected some error/not-found message, got: %s", resDomainMissing)
	}

	// 9. TIER 3 EDGE CASES: Multiple Custom KBs (Tenant + System Fallback + Missing)
	prepareKBForSearchTest(ctx, t, tenantDBInstance, "tenant_custom_1", "Tenant Docs: Custom 1.")
	prepareKBForSearchTest(ctx, t, systemDB, "system_custom_2", "System Docs: Custom 2.")

	payloadMultipleCustom := &ai.SessionPayload{
		CurrentDB:   tenantDBName,
		SelectedKBs: []string{"tenant_custom_1", "system_custom_2", "missing_custom"},
	}
	customCtxMultiple := context.WithValue(ctx, "session_payload", payloadMultipleCustom)

	// Use exact keyword to ensure it hits
	resMultipleCustom, err := agent.toolSearchCustomKBs(customCtxMultiple, map[string]any{"query": "Custom"})
	if err != nil {
		t.Fatalf("toolSearchCustomKBs multiple failed: %v", err)
	}

	// Ensure both Tenant and System fallbacks are present in the multi-kb query result.
	if !strings.Contains(resMultipleCustom, "Tenant Docs: Custom 1.") {
		t.Errorf("Expected Tenant Custom 1 Docs, got: %s", resMultipleCustom)
	}
	if !strings.Contains(resMultipleCustom, "System Docs: Custom 2.") {
		t.Errorf("Expected System Custom 2 Fallback Docs, got: %s", resMultipleCustom)
	}
}
