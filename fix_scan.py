import re

with open('ai/agent/atomic_engine.go', 'r') as f:
    content = f.read()

pattern = r"""storeVarName = e.resolveVarName\(storeVarName\)
\s*store,\s*ok\s*:=\s*e.getStore\(storeVarName\)
\s*if\s*!ok\s*\{
\s*return\s*nil,\s*fmt.Errorf\("store variable '%s' not found",\s*storeVarName\)
\s*\}"""

replacement = r"""storeVarName = e.resolveVarName(storeVarName)
store, err := e.getOrOpenStore(ctx, storeVarName)
if err != nil {
 nil, err
}"""

content = re.sub(pattern, replacement, content)

with open('ai/agent/atomic_engine.go', 'w') as f:
    f.write(content)
