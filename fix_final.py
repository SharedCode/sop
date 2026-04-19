with open('ai/agent/atomic_engine.go', 'r') as f:
    text = f.read()

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

text = text.replace("func (e *ScriptEngine) getStore(name string) (jsondb.StoreAccessor, bool) {", new_func)

scan_orig = """\tstoreVarName, _ := args["store"].(string)
\tstoreVarName = e.resolveVarName(storeVarName)
\tstore, ok := e.getStore(storeVarName)
\tif !ok {
\t\treturn nil, fmt.Errorf("\t\treturn nil, fmt.Errorf(nd\t\treturn nil, fmt.Errorf("\t\treturn nil, fmt.rN\t\treturn nis["store"].(string)
\tstoreVarName = e.resolveVarName(storeVarName)
\tstore, err := e.getOrOpenStore(ctx, storeVarName)
\tif e\tif e\tif e\tif e\tif e\tif e\tif ""\tif e\tif e\tif e\tif e\tif e\tif e\t_new)

with open('ai/wient/atomic_engine.go', 'w') as f:
    f.write(text)
