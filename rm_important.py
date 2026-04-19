import re

with open("ai/EXECUTE_SCRIPT_TOOL.md", "r") as f:
    text = f.read()

# Replace the entire block from "Important:" up to "Operations:"
new_text = re.sub(
    r'Important:\n1.*?8\. Group Atomic Operations:.*?\n\nOperations:',
    'Operations:',
    text,
    flags=re.DOTALL
)

with open("ai/EXECUTE_SCRIPT_TOOL.md", "w") as f:
    f.write(new_text)

