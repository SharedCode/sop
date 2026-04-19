import re
import os

with open("templates/scripts.html", "r") as f:
    lines = f.readlines()

start_idx = 0
for i, l in enumerate(lines):
    if '<script>' in l: 
        start_idx = i + 1; break

end_idx = len(lines)
for i in range(len(lines)-1, -1, -1):
    if '</script>' in lines[i]: 
        end_idx = i; break

js_lines = lines[start_idx:end_idx]

chunks = []
curr_chunk = []

for line in js_lines:
    strip_line = line.strip()
    # Simple heuristic: top level functions are typically indented 4 spaces or 0 spaces
    if len(curr_chunk) >= 800 and strip_line.startswith('function ') and (line.startswith('    function ') or line.startswith('function ')):
        chunks.append(curr_chunk)
        curr_chunk = []
    elif len(curr_chunk) >= 800 and strip_line.startswith('document.addEventListener'):
        chunks.append(curr_chunk)
        curr_chunk = []
        
    curr_chunk.append(line.rstrip('\n'))

if curr_chunk:
    chunks.append(curr_chunk)

print(f"Split {len(js_lines)} into {len(chunks)} chunks.")

replacement = ""
for i, chunk in enumerate(chunks):
    chunk_name = f"scripts_part{i+1:02d}"
    with open(f"templates/{chunk_name}.html", "w") as f:
        f.write(f'{{{{define "{chunk_name}"}}}}\n<script>\n')
        f.write("\n".join(chunk))
        f.write(f'\n</script>\n{{{{end}}}}\n')
    replacement += f'{{{{template "{chunk_name}" .}}}}\n'

with open("templates/index.html", "r") as f:
    text = f.read()

# Only replace if {{template "scripts" .}} exists
if r'{{template "scripts" .}}' in text:
    text = text.replace(r'{{template "scripts" .}}', replacement)

with open("templates/index.html", "w") as f:
    f.write(text)

print("index.html updated successfully.")
