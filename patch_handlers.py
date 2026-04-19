import re
import os

filepath = "tools/httpserver/main.go"
with open(filepath, "r") as f:
    orig_content = f.read()

content = orig_content

stores_pattern = r"func handleListStores\(w http\.ResponseWriter, r \*http\.Request\) \{.*?(?=func handleListKnowledgeBases)"
stores_replacement = """func handleListStores(w http.ResponseWriter, r *http.Request) {
\tw.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
\tdbName := r.URL.Query().Get("database")
\tctx := r.Context()
\tdbOpts, err := getDBOptions(ctx, dbName)
\tif err != nil {
\t\thttp.Error(w, err.Error(), http.StatusBadRequest)
\t\treturn
\t}

\tdb := aidb.NewDatabase(dbOpts)
\tvisibleStores, err := db.GetStores(ctx)
\tif err != nil {
\t\thttp.Error(w, "Failed to list stores: "+err.Error(), http.StatusInternalServerError)
\t\treturn
\t}

\tjson.NewEncoder(w).Encode(visibleStores)
}

"""

if "db := aidb.NewDatabase(dbOpts)" not in content:
    content = re.sub(stores_pattern, stores_replacement, content, flags=re.DOTALL)


kbs_pattern = r"func handleListKnowledgeBases\(w http\.ResponseWriter, r \*http\.Request\) \{.*?(?=func handleListKnowledgeThoughts)"
kbs_replacement = """func handleListKnowledgeBases(w http.ResponseWriter, r *http.Request) {
\tw.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
\tdbName := r.URL.Query().Get("database")
\tctx := r.Context()
\tdbOpts, err := getDBOptions(ctx, dbName)
\tif err != nil {
\t\thttp.Error(w, err.Error(), http.StatusBadRequest)
\t\treturn
\t}

\tdb := aidb.NewDatabase(dbOpts)
\tplaybooks, err := db.GetPlaybooks(ctx)
\tif err != nil {
\t\thttp.Error(w, "Failed to list playbooks: "+err.Error(), http.StatusInternalServerError)
\t\treturn
\t}

\tjson.NewEncoder(w).Encode(playbooks)
}

"""

if "db.GetPlaybooks" not in content:
    content = re.sub(kbs_pattern, kbs_replacement, content, flags=re.DOTALL)


if content != orig_content:
    with open(filepath, "w") as f:
        f.write(content)
    print("Patched " + filepath)
else:
    print("Already patched or pattern not found")
