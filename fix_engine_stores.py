import re

with open('ai/agent/atomic_engine.go', 'r') as f:
    content = f.read()

# Add getOrOpenStore
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

# Fix Scan explicitly
scan_pattern = r"""storeVarName, _ := args\["store"\].\(string\)
\s*storeVarName = e.resolveVarName\(storeVarName\)\s*storeVarName = e.resolveVarName\arName\)
\s*i\s*i\s*i\s*i\s*i\s*i\s*i\s*i\s*i\s*i\s*i\s*i\siabl\s*i\s*i\s*i\s*i\s*itoreVarName\)
\s*\}"""

scan_replace = r"""storeVarName, _ := args["store"].(string)
storeVarName = e.resolveVarName(storeVarName)
store, err := e.getOrOpenStore(ctx, storeVarNamstore, err := e.getOrOpenStore(ctx, st}"""
content = re.sub(sccontent = re.sub(sccontent = re.sub(sccontent = re.sub(sccojoincontent = re.surigcontent = re.sub(sccontent = re.sub(sccontent = re.sub(sccontent= r"""rightStore, isRightStore := e.gcontent = re.sub(sccontent = tScontent = re.sub(sccontent = re.sub(sccontent = re.sub(sccorightVar]; !ok {
:= e.getOrOpenStore(ctx, riif rs, err := e.getOrOpenStore(ctx, riif rs, err := e.getOrOpenStore(c}""":= e.getOrOpenStore(ctx, riif rs, err := e.getOrOpenStore(ctx, riif rs, err := e.getOrOpenace_stif rs, err := eretuif rs= m.group(1) # e.g. 'nil' or 'false'
    return f"""store    return f"""store    return f"""store    return f"""store    return f"""store    ret
}}"""

content = re.sub(
r"""store, ok := e\.getStore\(storeName\)
\s*if !ok \{\s*return([^,]\s*if !.Er\s*if !ok \{\s*return([^,]\s*if !.Er\s*if !ok \{\s*return([,
\s*if !ok \{\s*return([^,]\s*if !.Er\s*if !o/agent/atomic_engine.go', 'w') as f:
    f.write(content)
