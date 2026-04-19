import re

with open('ai/agent/atomic_engine.go', 'r') as f:
    content = f.read()

new_func = """func (e *ScriptEngine) getOrOpenStore(ctx context.Context, name string) (jsondb.StoreAccessor, error) {
\tresolved := e.resolveVarName(name)
\tif s, ok := e.getStore(resolved); ok {
\t\treturn s, nil
\t}
\ts, err := e.OpenStore(ctx, map[string]any{"name": resolved})
\tif err != nil {
\t\treturn nil, fmt.Errorf("store variable '%s' not found, and auto-open failed: %v", name, err)
\t}
\tif e.Context.Stores == nil {
\t\te.Context.Stores = make(map[string]jsondb.StoreAccessor)
\t}
\te.Context.Stores[resolved] = s
\treturn s, nil
}

func (e *ScriptEngine) getStore(name string) (jsondb.StoreAccessor, bool) {"""

content = content.replace("func (e *ScriptEngine) getStore(name string) (jsondb.StoreAccessor, bool) {", new_func)
with open('ai/agent/atomic_engine.go', 'w') as f:
    f.write(content)
