import re

with open('tools/httpserver/main.go', 'r') as f:
    content = f.read()

# Add handleListKnowledgeBases right after handleListStores
kbs_handler = """
func handleListKnowledgeBases(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
    dbName := r.URL.Query().Get("database")
    ctx := r.Context()
    dbOpts, err := getDBOptions(ctx, dbName)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
    if err != nil {
        http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
        return
    }
    defer trans.Rollback(ctx)

    stores, err := trans.GetStores(ctx)
    if err != nil {
        http.Error(w, "Failed to list stores: "+err.Error(), http.StatusInternalServerError)
        return
    }

    // A Vector store creates several sub-stores. We    // A Vector store crea to identify the     // A Vector store creates several sub-stobool    // A Vector strange     // A Vector store creates several sub-stores. We            // A Vectostri    // A Vector store creates several sub-stores. We    // A Vector st      // A Vector store creates several sub-stores. We    // A Vector sto    result = append(result, kb)
    }

    json.NewEncoder(w).Encode(result)
}
"""

content = content.replace('func handleGetDBOptions', kbs_handler + '\nfunc handleGetDBOptions')

# Register th# Registen main# Reout# Register th# Registen main# Reout# Register th# RegistleListStores)
httpserverhththeFunc("/api/knowledge-bases", handleListKnowledgeBases)'''

content = content.replace('http.HandleFunc("/api/stcontent = dleLicontorcontent = content.replace('hithcontent = content.replace('http., 'w') acontent = contte(content)
