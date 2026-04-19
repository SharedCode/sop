import sys, re

with open('main.go', 'r') as f:
    text = f.read()

pattern = r'(var visibleStores \[\]string.*?for _, s := range stores {.*?if ).*?(\{.*?visibleStores = append\(visibleStores, s\).*?\}.*?\}.*?json\.NewEncoder\(w\)\.Encode\(visibleStores\)\n\})'

def repl(m):
    return m.group(1) + '!strings.Contains(s, "/") && !strings.HasSuffix(s, "_vecs") ' + m.group(2)

new_text = re.sub(pattern, repl, text, flags=re.DOTALL)
with open('main.go', 'w') as f:
    f.write(new_text)

