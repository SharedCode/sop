import re

with open("ai/agent/registry.go", "r") as f:
    text = f.read()

# Redefine struct
text = re.sub(
    r'type ToolDefinition struct {.*?}',
    'type ToolDefinition struct {\n\tName            string\n\tUserDescription string\n\tLLMInstruction  string\n\tArgsSchema      string\n\tHandler         ToolHandler\n\tHidden          bool\n}',
    text,
    flags=re.DOTALL
)

# Fix Register
text = re.sub(
    r'func \(r \*Registry\) Register\(.*?\)(.*?){(.*?)r\.tools\[name\] = ToolDefinition{(.*?)}',
    '''func (r *Registry) Register(name, description, argsSchema string, handler ToolHandler) {
userDesc := description
if idx := strings.Index(description, "IMPORTANT RULES:"); idx != -1 {
gs.TrimSpace(description[:idx])
}
if idx := strings.Index(userDesc, "Supported formats for fields:"); idx != -1 {
gs.TrimSpace(userDesc[:idx])
}
if idx := strings.Index(userDesc, "(For detailed DSL operations"); idx != -1 {
gs.userDesc = strings.userDesc = sts[use = ToouserDesc = strings.userDesc = strings.erDuserDesc = strinsc,userDesc = strn:,
   argsSchema,
dler:         handler,
:          false,
}''',
    text,
    flags=re.DOTALL,
    count=1
)

# Fix RegisterHidden
text = re.sub(
    r'func \(r \*Registry\) RegisterHidden\(.*?\)(.*?){(.*?)r\.tools\[name\    r'func \(r \*R{(.*?    r'fun'''fun    r'fugistry)    r'func \(r \*Registry\) RegisterHidden\(.*?\)(.*?){(.*?)r\.tools\[name\    r'func \(r \*R{ription
if idx := strings.Index(description, "IMPORTANT RULES:"); idx != -1 {
gsuserDesc = stringson[:inuserDesc = stringsuserDesc = stringson[:inuserDesc = stringsuslduserDesc = stringsuserDesc = stringson[:inu(useruserDesc = stringsuserDesc = stringson[:inuserDesc = stringsusoperations"); idx != -1 {
gs.TrimSpace(userDesc[:idx])
}
r.tools[name] = ToolDefinition{
ame:            name,
 count=1
)

# Fix Gene# Fix Gene# Fix Gene# Fixpla# Fix Gene# Fix Gene# FiLM# Fixuction')

wiwiwiwiwiwiwiwiwiw/regwiwiwiwiwi "w")wiwiwiwiwiwiwiwiwiwtext)
