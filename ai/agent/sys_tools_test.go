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
	if strings.Contains(tools, "<h2> Example</h2>") || strings.Contains(tools, "join_right") {
		t.Fatalf("expected injected Stores tools context to omit the large example block, got: %s", tools)
	}
	if !strings.Contains(tools, "Research & Orchestration Rules") || !strings.Contains(tools, "Use `list_stores` to research schema and relations") {
		t.Fatalf("expected injected Stores tools context to retain compact research guidance, got: %s", tools)
	}
}
