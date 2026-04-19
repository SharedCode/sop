import re
import os

input_file = "templates/scripts.html"
with open(input_file, "r") as f:
    text = f.read()

lines = text.split("\n")

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

js_lines = lines[start_idx:end_idx]

chunks = []
curr_chunk = []
depth = 0

def calculate_depth(line, current_depth):
    cl = line
    cl = re.sub(r'//.*$', '', cl)
    opens = cl.count('{')
    closes = cl.count('}')
    return current_depth + opens - closes

in_block_comment = False
for line in js_lines:
    if "/*" in line: in_block_comment = True
    
    if not in_block_comment:
        depth = calculate_depth(line, depth)
        
    if "*/" in line: in_block_comment = False
    
    curr_chunk.append(line)

depth += line.count('{') - line.count('}')
    if depth == 0 and len(curr_chunk) >= 800 and ('    function ' in line or 'function ' in line):
            # pop the last line since it belongs to the next chunk
            if "function" in curr_chunk[-1]:
                last_line = curr_chunk.pop()
                chunks.append(curr_chunk)
                curr_chunk = [last_line]
            else:
                chunks.append(curr_chunk)
                curr_chunk = []

if curr_chunk:
    chunks.append(curr_chunk)

print(f"Split into {len(chunks)} chunks.")

replacement = ""
for i, chunk in enumerate(chunks):
    chunk_name = f"scripts_part{i+1:02d}"
    with open(f"templates/{chunk_name}.html", "w") as f:
        f.write(f'{{{{define "{chunk_name}"}}}}\n<script>\n')
        f.write("\n".join(chunk))
        f.write(f'\n</script>\n{{{{end}}}}\n')
    replacement += f'{{{{template "{chunk_name}" .}}}}\n'

with open("templates/index.html", "r") as f:
    index_html = f.read()

index_html = re.sub(r'\{\{template "scripts" \.\}\}', replacement, index_html)

with open("templates/index.html", "w") as f:
    f.write(index_html)

print("Done")
