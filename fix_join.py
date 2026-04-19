import re

with open('ai/agent/atomic_engine.go', 'r') as f:
    content = f.read()

# For Join:
# rightStore, isRightStore := e.getStore(rightVar)
# Wait, if isRightStore evaluates to false, we can check if it exists as a variable.
# If it DOES NOT exist as a variable, we can try getOrOpenStore.
replacement = r"""rightStore, isRightStore := e.getStore(rightVar)
if !isRightStore {
:= e.Context.Variables[rightVar]; !ok {
not a memory variable, try auto-opening it as a store
:= e.getOrOpenStore(ctx, rightVar); err == nil {
= true
tent = re.sub(r'rightStore,\s*isRightStore\s*:=\s*e.getStore\(rightVar\)', replacement, content)

# I should also do the same for JoinRight (which is the flipped version)
with open('ai/agent/atomic_engine.go', 'w') as f:
    f.write(content)
