import re

with open("ai/agent/copilottools.go", "r") as f:
    text = f.read()

exec_inst = """const ExecuteScriptInstruction = `Execute a programmatic script to interact with databases. Use this for complex multi-step operations not covered by high-level tools. (For detailed DSL operations, refer to your knowledge base).
IMPORTANT RULES:
1. ONLY use valid script operations explicitly defined in the AST Grammar.
2. DO NOT use high-level tools (like 'select' or 'add') inside execute_script unless explicitly allowed.
3. Every store MUST be explicitly opened with 'open_store' before querying.
4. Inspect Schema First: Use 'list_stores' to discover stores, field names (often using '_' like 'total_amount'), and plural/singular store names.
5. Inspect store "relations" to determine correct join logic and optimized access paths.
6. When joining using a Secondary Index, respect the field names in the 'Relation'. If a Relation maps '[Value]' to 'target_id', use 'Value' in your 'on' cla6. When joining using a Seconurn full object6. When joining using a Secondary Index, respe'project' step. If reque6. When joining using a Secondary Index, respect the field names in the 'Relation'. If a Relation maps '[Value]' to 'target_id', use 'Value' in your 'on' cla6. When joining using a Seconurn full object6. When joining using a Secondary Index, 

text = re.sub(
                                                                           lags=re.DOTALL
)

with open("ai/agent/copilottools.go", "w") as f:
    f.write(text)

