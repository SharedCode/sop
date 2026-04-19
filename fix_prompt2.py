import re

with open("ai/agent/copilottools.go", "r") as f:
    text = f.read()

new_rule3 = "3. EVERY script MUST start with 'begin_tx' (e.g. {\"op\": \"begin_tx\", \"result_var\": \"tx1\"}) and end with 'commit_tx'."
new_rule4 = "4. Pass the transaction variable (e.g. \"tx1\") to 'open_store' via the 'transaction' argument."

text = text.replace(
    "3. EVERY script MUST start with 'begin_tx' (assigning result_var) and end with 'commit_tx'. Pass the transaction var to 'open_store' via args.\n4. 'scan' and 'join'",
    f"{new_rule3}\n{new_rule4}\n5. 'scan' and 'join'"
)

# And renumber
text = text.replace("5. When joining", "6. When joining")
text = text.replace("6. Group atomic operations", "7. Group atomic operations")
text = text.replace("7. Inspect Schema First", "8. Inspect Schema First")

with open("ai/agent/copilottools.go", "w") as f:
    f.write(text)
