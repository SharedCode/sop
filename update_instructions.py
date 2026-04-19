import re

with open("ai/agent/copilottools.go", "r") as f:
    text = f.read()

# Refine ExecuteScriptInstruction to pull rules back from MD
exec_inst = """const ExecuteScriptInstruction = `Execute a programmatic script to interact with databases. Use this for complex multi-step operations not covered by high-level tools. (For detailed DSL operations, refer to your knowledge base).
IMPORTANT RULES:
1. ONLY use valid script operations explicitly defined in the AST Grammar.
2. DO NOT use high-level tools (like 'select' or 'add') inside execute_script unless explicitly allowed.
3. Every store MUST be explicitly opened with 'open_store' before querying.
4. 'scan' and 'join' return full objects. To project specific fields, you MUST add a 'project' step.
5. When joining using a Secondary Index, respect the field names in the 'Relation'. e.g., if it maps '[Value]' to 'target_id', use 'Value' in your 'on' clause.
6. Group atomic operations (scan, filter, join, project) into a s6. Group atomic operations (scan, filec6. Groua Fi6. Group atomic operations (scan, filter, join,ld6. Group at relations before scripting.`"""

# Find and replace ExecuteScriptInstr# Find and replace ExecuteScriptInstr# Find and replace ExecuteScriptInstr# Find and replace ExecuteScriptInstr# Find and replace ExecuteScriptInstr# Find and replace ExecuteScriptInstr# Find and replace ExecuteScriptInstr# Find and replace E, [\'field1\', \'fi# Find and replace ExecuteScriptInstr# Find and replace ExecuteScript order# Find and replace ExecuteScriptInstr# Find and replace ExecuteScriptInstr# Find and replace o sort by other fields, you must use execute_script with an index store."'

text = re.sutext = re.sutext = re.sutext = re.sutext = re.sutext = re.sutext = re.suct_inst,
    text,
    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flag\'    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flags=    flagsary key."'
text = re.sub(
    r'JoinInstruction = "Joins data from two stores\..*?"',
    join_inst,
    text,
    flags=re.DOTALL
)

with open("ai/agent/copilottools.go", "w") as f:
    f.write(text)

