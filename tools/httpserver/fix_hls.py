import sys

with open('main.go', 'r') as f:
    text = f.read()

# We need to find `stores, err := trans.GetStores(ctx) ...` down to `func handleListKnowledgeBases`
import re
pattern = r'(stores, err := trans\.GetStores\(ctx\).*?)(func handleListKnowledgeBases)'
def repl(m):
    return """stores, err := trans.GetStores(ctx)
        if err != nil {
                http.Error(w, "Failed to list stores: "+err.Error(), http.StatusInternalServerError)
                return
        }

        var visibleStores []string
        for _, s := range stores {
                if !strings.HasSuffix(s, "_vecs") && !strings.HasSuffix(s, "/vecs") {
                        visibleStores = append(visibleStores, s)
                }
        }

        json.NewEncoder(w).Encode(visibleStores)
}

""" + m.group(2)

new_text = re.sub(pattern, repl, text, flags=re.DOTALL)
with open('main.go', 'w') as f:
    f.write(new_text)
