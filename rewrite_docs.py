import re

with open('ai/EXECUTE_SCRIPT_TOOL.md', 'r') as f:
    text = f.read()

operations_start = text.find('Operations:\n')
example_start = text.find('Example Pipeline Join:\n')

new_operations = """Operations:
- open_db(name) -> db
- begin_tx(database, mode) -> tx
- commit_tx(transaction)
- rollback_tx(transaction)
- open_store(transaction, name) -> store
- scan(store, limit, direction ("asc" or "desc"), start_key, prefix, filter, stream=true) -> cursor
- sort(input, fields) -> list
- filter(input, condition) -> cursor/list
- project(input, fields) -> cursor/list
- limit(input, limit) -> cursor/list
- join(input, with, type, on) -> cursor/list
- join_right(input, store, type, on) -> cursor/list (Pipeline alias for join)
- update(input, store) -> bulk updates the incoming piped list of records in the store
- delete(input, store) -> bulk deletes the incoming piped list of records from the store
- if(condition, then, else)
- loop(condition, body)
- call_script(name, params)
- return(value) -> stops execution and returns value

"""

# Print out some debugging
print(f"Start: {operations_start}, End: {example_start}")

if operations_start != -1 and example_start != -1:
    text = text[:operations_start] + new_operations + text[example_start:]
    print("Replaced!")
else:
    print("Could not find delimiters.")

with open('ai/EXECUTE_SCRIPT_TOOL.md', 'w') as f:
    f.write(text)