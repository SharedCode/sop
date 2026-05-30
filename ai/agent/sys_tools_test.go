package agent

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/memory"
)

func TestSystemTools_MRU_Integration(t *testing.T) {
	ctx := context.Background()

	os.RemoveAll("./test_data/sys_tools")
	defer os.RemoveAll("./test_data/sys_tools")
	sysDBOptions := sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{"./test_data/sys_tools"}}
	sysDB := database.NewDatabase(sysDBOptions)

	cfg := Config{}
	ag := NewCopilotAgent(cfg, map[string]sop.DatabaseOptions{}, sysDB)

	ag.service = &Service{
		session: &RunnerSession{
			MRU: []MRUItem{},
		},
	}
	ag.Memory = memory.NewMemoryUnit("test_agent")

	// 1. Fake the determinisic injection from gateway
	ag.MarkMRUCategory("System_Tools", "MRU_EXCLUSIVELY_SERVED_THIS")

	// 2. Fetch the tools - should strictly read from MRU without LTM trip
	toolsDef2 := ag.getSystemToolsContext(ctx)
	if toolsDef2 != "MRU_EXCLUSIVELY_SERVED_THIS" {
		t.Fatalf("Expected getSystemToolsContext to fetch strictly from MRU, got: %s", toolsDef2)
	}
}

func TestSystemTools_OmniAndAvatar_Injections(t *testing.T) {
	ctx := context.Background()

	os.RemoveAll("./test_data/sys_tools_inj")
	defer os.RemoveAll("./test_data/sys_tools_inj")
	sysDBOptions := sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{"./test_data/sys_tools_inj"}}
	sysDB := database.NewDatabase(sysDBOptions)

	cfg := Config{}
	ag := NewCopilotAgent(cfg, map[string]sop.DatabaseOptions{}, sysDB)

	ag.service = &Service{
		session: &RunnerSession{
			MRU: []MRUItem{},
		},
	}
	ag.Memory = memory.NewMemoryUnit("test_agent")

	// Inject System Tool to MRU manually for prompt validation
	ag.MarkMRUCategory("System_Tools", "INJECTED_SYSTEM_TOOL_DEFINITION")

	// Must set payload for Avatar compilation rules
	p := &ai.SessionPayload{
		CurrentDB: SystemDBName, // avoid nil deref
	}
	sessionCtx := context.WithValue(ctx, "session_payload", p)

	omniPrompt := ag.buildSystemPrompt(sessionCtx, "Test user query", TaskContextClassification{})
	if !strings.Contains(omniPrompt, "INJECTED_SYSTEM_TOOL_DEFINITION") {
		t.Fatalf("OMNI Prompt failed to include System Tools from MRU/getSystemToolsContext")
	}

	avatarPrompt := ag.buildAvatarPrompt(sessionCtx, "data_viz", "generate me a chart", "", []string{"select"})
	if !strings.Contains(avatarPrompt, "INJECTED_SYSTEM_TOOL_DEFINITION") {
		t.Fatalf("Avatar Prompt failed to include System Tools from MRU/getSystemToolsContext")
	}
}

func TestInjectToolsForDomain_StoresUsesCompactProtocolSlice(t *testing.T) {
	ctx := context.Background()
	sysDBOptions := sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{t.TempDir()}}
	sysDB := database.NewDatabase(sysDBOptions)

	ag := NewCopilotAgent(Config{}, map[string]sop.DatabaseOptions{}, sysDB)
	ag.service = &Service{session: &RunnerSession{MRU: []MRUItem{}}}

	ag.injectToolsForDomain(ctx, &TaskContextClassification{Domain: StoresDomain})
	tools := ag.getSystemToolsContext(ctx)

	if !strings.Contains(tools, "Structured Context: Stores Tools") {
		t.Fatalf("expected Stores tools context to be injected, got: %s", tools)
	}
	if strings.Contains(tools, "<h2> Example</h2>") || strings.Contains(tools, "Execution Flow Engine Guardrails") {
		t.Fatalf("expected injected Stores tools context to omit the large example block, got: %s", tools)
	}
	if !strings.Contains(tools, "Never guess store names") || !strings.Contains(tools, "Think through the read/join/filter plan") {
		t.Fatalf("expected injected Stores tools context to retain the simplified protocol guidance, got: %s", tools)
	}
	if !strings.Contains(tools, "Treat transaction boundaries as first-class") || !strings.Contains(tools, "commit or rollback") {
		t.Fatalf("expected injected Stores tools context to preserve explicit transaction control guidance, got: %s", tools)
	}
	if !strings.Contains(tools, "business-critical") || !strings.Contains(tools, "persist together") {
		t.Fatalf("expected injected Stores tools context to explain why explicit transactions matter for data mutation sets, got: %s", tools)
	}
	if !strings.Contains(tools, "50 to 250 CRUD operations per transaction") || !strings.Contains(tools, "then commit") {
		t.Fatalf("expected injected Stores tools context to encourage deliberate batching around explicit commits, got: %s", tools)
	}
	if !strings.Contains(tools, "retry once") || !strings.Contains(tools, "short clarification question") {
		t.Fatalf("expected injected Stores tools context to include retry and clarification guidance, got: %s", tools)
	}
}

func TestInjectToolsForDomain_SpacesUsesDescriptionContext(t *testing.T) {
	ctx := context.Background()
	sysDBOptions := sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{t.TempDir()}}
	sysDB := database.NewDatabase(sysDBOptions)

	ag := NewCopilotAgent(Config{}, map[string]sop.DatabaseOptions{}, sysDB)
	ag.service = &Service{session: &RunnerSession{MRU: []MRUItem{}}}

	ag.injectToolsForDomain(ctx, &TaskContextClassification{Domain: SpacesDomain})
	tools := ag.getSystemToolsContext(ctx)

	if !strings.Contains(tools, "Structured Context: Spaces Tools") {
		t.Fatalf("expected Spaces tools context to be injected, got: %s", tools)
	}
	if !strings.Contains(tools, "- mint_to_space:") || !strings.Contains(tools, "- update_space_config:") {
		t.Fatalf("expected injected Spaces tools context to be generated from tool descriptions, got: %s", tools)
	}
	if !strings.Contains(tools, "manages its own write transaction") || !strings.Contains(tools, "explicitly asks for vectorization or semantic refresh") {
		t.Fatalf("expected injected Spaces tools context to retain behavioral guidance from tool descriptions, got: %s", tools)
	}
	if !strings.Contains(tools, "business-critical") || !strings.Contains(tools, "defines the durability boundary") {
		t.Fatalf("expected injected Spaces tools context to explain why transaction boundaries matter for persisted knowledge changes, got: %s", tools)
	}
	if strings.Contains(tools, "# Spaces Manual") || strings.Contains(tools, "<h2> Core Conventions</h2>") {
		t.Fatalf("expected injected Spaces tools context to avoid the old manual blob, got: %s", tools)
	}
}
