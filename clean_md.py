import re

with open("ai/EXECUTE_SCRIPT_TOOL.md", "r") as f:
    text = f.read()

# Strip the "## execute_script Tool Instruction" and its paragraph up to "Operations:"
text = re.sub(
    r'## execute_script Tool Instruction\nExecute a programmatic script.*?Operations:',
    '## execute_script Tool Operations:',
    text,
    flags=re.DOTALL
)

# Strip the redundant Note and JSON AST Grammar Constraint at the bottom
text = re.sub(
    r'Note: \'scan\' and \'join\' return full objects\..*?\]\n\}\n```\n?',
    '',
    text,
    flags=re.DOTALL
)

with open("ai/EXECUTE_SCRIPT_TOOL.md", "w") as f:
    f.write(text)

