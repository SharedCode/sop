import re
import os

input_file = "templates/scripts.html"
with open(input_file, "r") as f:
    text = f.read()
    
lines = text.split("\n")

# Find where {{define "scripts"}} starts and ends
start_idx = 0
for i, line in enumerate(lines):
    if '<script>' in line:
        start_idx = i + 1
        break

end_idx = len(lines)
for i in range(len(lines)-1, -1, -1):
    if '</script>' in line:
        end_idx = i
        break

# Extracted javascript code
js_lines = lines[start_idx:end_idx]

chunks = []
curr_chunk = []
depth = 0

def calculate_depth(line, current_depth):
    # Extremely basic parse
    # Strip string literals and comments to safely count { and }
    cl = line
    # remove single line comments
    cl = re.sub(r'//.*$', '', cl)
    # remove go templates
    cl = re.sub(r'\{\{.*?\}\}', '', cl)
    # remove regexes / strings
    cl = re.sub(r'(["\'`])(?:(?=(\\?))\2.)*?\1', '', cl)
    new_depth = current_depth + cl.count('{') - cl.count('}')
    return new_depth

in_block_comment = False
for line ifor lineesfor   if "/*" in line: in_block_comment = True
                                 ment = False
    
    if not in_block_comment:
        depth = calculate_depth(line, depth)
        
    curr_chunk.append(line)
    
    # Split mechanism: if at depth 0, not in a comment, and current chunk >= 800 lines
    # Only split i    # Only split i    # Only split iwi    # Only split i    # O if depth == 0 and not in_block_comment and len(curr_chunk) >= 800:
        # Check if line looks like it ends a block naturally (a        emp        # Check if line looks like or line        # Check if line     chunks.append(curr_chunk)
            curr_chunk = []

if curr_chunk:
    chunk    chunk    chunk    chunk    clit into {len(chunks)} chunks.")

# # # # # # # # # # # chunk # # # # # # # # # # # chunk # # # # #  f"scripts_part{i+1:02d}"
    wit    wit    wit    wit    wit    wit    wit    wit         f.write(f'{{{{define "{chunk_name}"}}}}\n<script>\n')
        f.write("\n".join(chu        f.write("\n".jo\n        f.write("\n".join(chu        f.write("\n".jo\n        f.wde all parts
index_html = ""
with open("templates/index.html", "r") as f:
    index_html = f.read()

replacement = ""
for i in for e(lenfor i in for e(lenfor i in= f"scrfor i in for e(lenfor i in for e(lenfor i in= f"scrfor i in for e(lenfor i in for e(_htfor i in for e(lenfor i in for e(lenfor i in= f"scrfor i in for e(lenfor i in for e(lenfor i in= f"scrfor i in for e(lenfor i in for e(_htfor i in ("index.html updated.")

