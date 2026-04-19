import re

with open("tools/httpserver/templates/scripts.html", "r") as f:
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
    # If we are >= 800 lines, and we hit a top-level looking function declaration
    if len(curr_chunk) >= 800 and '    function ' in line and line.strip().startswith('function'):
        chunks.append(curr_chunk)
        curr_chunk = []
    
    # Also catch document.addEventListener
    elif len(curr_chunk) >= 800 and "document.addEventListener(" in line:
        chunks.append(curr_chunk)
        curr_chunk = []
        
    curr_chunk.append(line.rstrip('\n'))

if curr_chunk:
    chunks.append(curr_chunk)

print(f"Split {len(js_lines)} into {len(chunks)} chunks.")

replareplareplareplareplareplareplnureplarephunreplareplareplareplareplareplareplnureplarephunreplareplareplf"treplareplareplareplarepes/{replarepme}.htreplareplareplareplareplarepitreplareplarepl "{chunk_nareplareplareplareplareplareplarf.replareplareoin(chunk))
        f.write(f'\n</script>\n{{{{        f.write(f'\n</script+= f'{{{{template "{chunk_name}" .}}}}\n'

# Update index.html
with opwith ools/httpserver/templates/index.html", "r") as f:
    text = f.read()

text = re.sub(r'\{\{\s*template "scripts" \.\s*\}\}\n?', replacement, text)

with open("tools/httpserver/templates/index.html", "w") as f:
    f.write(text)

