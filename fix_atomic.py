import re

with open('ai/agent/atomic_engine.go', 'r') as f:
    content = f.read()

helper = """func (e *ScriptEngine) getOrOpenStore(ctx context.Context, name string) (jsondb.StoreAccessor, error) {
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

content = content.replace("func (e *ScriptEngine) getStore(name string) (jsondb.StoreAccessor, bool) {", helper)

# Now find all getStore instances and replace!

# Pattern: 
# storeVarName = e.resolveVarName(storeVarName)
# store, ok := e.getStore(storeVarName)
# if !ok {
#         #         #         #       e #        '%#         #         #         #       e #       ix#         #    en Join, #         #         #         #       e #        '%#         #         #         #       e #       ix#         #   e.getStore\(storeVarName\)
\s*if !ok \{
\s*return nil, fmt.Errorf\("store variable '%s' not found", s\s*return nil, fmt.Errorf\("store variable '%s' not found"(ctx,\s*retuarName)
ifififififififififififififififififififififififififififififififi"store"].ifififififififififiStore(storeName)
# if !ok { return <return_type>, fmt.Errorf(...) }

# Since return type varies, let# Since rehon scrip# Since return type varies, let# Since rehon scrip# Since return type varies,il' or 'false'
    return f"""store, err := e.getOrOpenStore(ctx, storeName)
if err != nil {{
 {return_vals}return {return_vals}return {return_val ok := e\.getStorereturn {return_vals}return {return_vals}t.return {return_vals}return {retund",returame\)
\s*\}""",
replace_store_check, content)

with open('ai/agent/atomic_engine.go', 'w') as f:
    f.write(content)

