package agent

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// ToolHandler is the function signature for a tool execution.
type ToolHandler func(ctx context.Context, args map[string]any) (string, error)

// ToolDefinition defines a tool's metadata and handler.
type ToolDefinition struct {
	Name        string
	Description string
	ArgsSchema  string // JSON schema or description of args
	Handler     ToolHandler
	Hidden      bool
}

// Registry manages the available tools.
type Registry struct {
	tools map[string]ToolDefinition
}

// NewRegistry creates a new tool registry.
func NewRegistry() *Registry {
	return &Registry{
		tools: make(map[string]ToolDefinition),
	}
}

// Register adds a tool to the registry.
func (r *Registry) Register(name, description, argsSchema string, handler ToolHandler) {
	r.tools[name] = ToolDefinition{
		Name:        name,
		Description: description,
		ArgsSchema:  argsSchema,
		Handler:     handler,
		Hidden:      false,
	}
}

// RegisterHidden adds a hidden tool to the registry (not shown in prompt).
func (r *Registry) RegisterHidden(name, description, argsSchema string, handler ToolHandler) {
	r.tools[name] = ToolDefinition{
		Name:        name,
		Description: description,
		ArgsSchema:  argsSchema,
		Handler:     handler,
		Hidden:      true,
	}
}

// Get retrieves a tool definition by name.
func (r *Registry) Get(name string) (ToolDefinition, bool) {
	t, ok := r.tools[name]
	return t, ok
}

// List returns all registered tools.
func (r *Registry) List() []ToolDefinition {
	var list []ToolDefinition
	for _, t := range r.tools {
		list = append(list, t)
	}
	// Sort by name for deterministic output
	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})
	return list
}

// GeneratePrompt generates the tools description for the LLM prompt.
func (r *Registry) GeneratePrompt() string {
	var sb strings.Builder
	sb.WriteString("You have access to the following tools to help answer the user's question.\n")
	sb.WriteString("To use a tool, you MUST output a JSON object in the following format ONLY, with no other text:\n")
	sb.WriteString("{\"tool\": \"tool_name\", \"args\": { ... }}\n\n")
	sb.WriteString("Tools:\n")

	for i, t := range r.List() {
		if t.Hidden {
			continue
		}
		sb.WriteString(fmt.Sprintf("%d. %s%s - %s\n", i+1, t.Name, t.ArgsSchema, t.Description))
	}
	return sb.String()
}
