import sys

with open('tools/httpserver/main.go', 'r') as f:
    lines = f.readlines()

new_lines = []
for i, line in enumerate(lines):
    new_lines.append(line)

# Let's cleanly replace the problem block.
# We know it starts at `visibleKBs := make(map[string]bool)`
# and ends at `json.NewEncoder(w).Encode(result)`

block = """        visibleKBs := make(map[string]bool)
        for _, s := range stores {
                if strings.HasSuffix(s, "/vecs") {
                        kbName := strings.TrimSuffix(s, "/vecs")
                        visibleKBs[kbName] = true
                } else if strings.HasSuffix(s, "_vecs") {
                        kbName := strings.TrimSuffix(s, "_vecs")
                        visibleKBs[kbName] = true
                }
        }

        var result []string
        for kb := range visibleKBs {
                result = append(result, kb)
        }

        json.NewEncoder(w).Encode(result)
}
"""

with open('tools/httpserwith open('tools/httpserwith open('tools/httpserwort re
pattern = pattern = pattern = pattern = pattern = patternn\.NewEncoder\(w\)\.Encode\(rpattern =\}'
text = re.sub(pattern, block, text,text = re.sub(patt
with open('tools/httpserver/main.go', 'w') as f:
    f.write(text)

