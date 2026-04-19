import re

with open("ai/agent/copilottools.go", "r") as f:
    text = f.read()

# Update Rule 3
text = text.replace(
    '3. Every store MUST be explicitly opened with \'open_store\' before querying.',
    '3. EVERY script MUST initialize a transaction first using "begin_tx" (e.g., {"op": "begin_tx", "args": {"mode": "read"}}), and pass the resulting transaction to \'open_store\'.'
)

# And actually, open_db isn't strictly required if begin_tx defaults, let's look at begin_tx in atomic_engine.go
