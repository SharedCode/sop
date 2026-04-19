import sys
import re

with open('ai/agent/atomic_engine.go', 'r') as f:
    text = f.read()

pattern1 = r"""store, ok := e\.getStore\(storeName\)
if !ok \{
 nil, fmt\.Errorf\("store variable '%s' not found", storeName\)
\}"""
replace1 = r"""store, err := e.getOrOpenStore(ctx, storeName)
if err != nil {
 nil, err
}"""
text = re.sub(pattern1, replace1, text)

pattern2 = r"""store, ok := e\.getStore\(storeName\)
if !ok \{
 false, fmt\.Errorf\("store variable '%s' not found", storeName\)
\}"""
replace2 = r"""store, err := e.getOrOpenStore(ctx, storeName)
if err != nil {
 false, err
}"""
text = re.sub(pattern2, replace2, text)

with open('ai/agent/atomic_engine.go', 'w') as f:
    f.write(text)
