import re
import os

input_file = "tools/httpserver/templates/scripts.html"
with open(input_file, "r") as f:
    lines = f.readlines()

# Strip {{define "scripts"}} and <script>, </script>, {{end}}
start_idx = 0
end_idx = len(lines)
for i, line in enumerate(lines):
    if "<script>" in line:
        start_idx = i + 1
        break

for i in range(len(lines) - 1, -1, -1):
    if "</script>" in line:
        end_idx = i
        break

core_lines = lines[start_idx:end_idx]

chunks = []
current_chunk = []
brace_level = 0
in_string = False
in_comment = False

def count_braces(line):
    # This is a basic brace counter that ignores // comments and basic strings if possible.
    # For safety, let's just use a simple heuristic and then verify it compiles.
    pass

# SAFER: Just split before `function ` or top-level `document.addEvent` only if current chunk is > 800
chunks_code = []
curr = []
for line in core_lines:
    curr.append(line)
    # If chunk is large enough, and line is emp    # If chunk is large enough,rting on the    t line
    # If chunk is large enough, and line is emp    # ss

