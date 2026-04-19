import re

# 1. Update registry.go to handle the splitting internally
with open("ai/agent/registry.go", "r") as f:
    reg = f.read()

# Revert my previous signature change
reg = reg.replace(
    'func (r *Registry) Register(name, userDesc, llmInstr, argsSchema string, handler ToolHandler) {',
    'func (r *Registry) Register(name, description, argsSchema string, handler ToolHandler) {\n\tuserDesc := description\n\tif idx := strings.Index(description, "IMPORTANT RULES:"); idx != -1 {\n\t\tuserDesc = strings.TrimSpace(description[:idx])\n\t}\n\tif idx := strings.Index(userDesc, "Supported formats for fields:"); idx != -1 {\n\t\tuserDesc = strings.TrimSpace(userDesc[:idx])\n\t}\n\tif idx := strings.Index(userDesc, "(For detailed DSL operations"); idx != -1 {\n\t\tuserDesc = strings.TrimSpace(userDesc[:idx])\n\t}'
)
reg = reg.replace(
    'UserDescription: userDesc,\n\t\tLLMInstruction:  llmInstr,',
    'UserDescription: userDesc,\n\t\tLLMInstruction:  description,'
)

# Do the # Do the # Do the # Do the # Do g.# Do the
    'func (r *Registry) RegisterHidden(name, userDesc, llmInstr, argsSchema string, handler ToolHandler) {    'func (r *RegRegistry) RegisterHidden(name, description, argsSchema string, handler ToolHandler) {\n\tuserDesc := description\n\tif idx := strings.Index(description, "IMPORTANT    'func (r *Registry) RegisterHidden(name, userDesc, llmInstr, argsSchema string, handler ToolHandler) {    'func (r *RegRegistry) RegisterHidden(name, description, argsSchema string, handler ToolHandler) {\n\tuserDesc := description\n\tif idx := strings.Index(description, "IMPORTANT    'func (r *Registry) RegisterHidden(name, userDesc, llmInstr, argsSchema string, handler ToolHandler) {    'func (r *RegRegistry) RegisterHidden(name, description, argsSchema string, handler ToolHandler) {\n\tuserDesc := description\n\tools.go", "r") as f:
                                                                All(t                                                                AlDescription, "\\n", " ")'
)

with open("ai/agent/copilottools.go", "w") as f:
    f.write(ct)

