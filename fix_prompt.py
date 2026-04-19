import re

with open("ai/agent/copilottools.go", "r") as f:
    text = f.read()

new_rule3 = "3. EVERY script MUST start with 'begin_tx' (assigning result_var) and end with 'commit_tx'. Pass the transaction var to 'open_store' via args."

text = text.replace(
    "3. Every store MUST be explicitly opened with 'open_store' before querying.",
    new_rule3
)

with open("ai/agent/copilottools.go", "w") as f:
    f.write(text)

