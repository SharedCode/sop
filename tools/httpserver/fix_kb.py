import re

with open("tools/httpserver/knowledge_manager.go", "r") as f:
    content = f.read()

# 1. Add URL to struct
struct_regex = re.compile(r'type PreloadKnowledgeRequest struct \{[\s\S]*?\}')

new_struct = """type PreloadKnowledgeRequest struct {
Expertise         string          `json:"expertise_id"`
DatabaseName      string          `json:"database_name"`
KnowledgeBaseName string          `json:"knowledge_base_name,omitempty"`
URL               string          `json:"url,omitempty"`
CustomData        json.RawMessage `json:"custom_data,omitempty"`
}"""

content = struct_regex.sub(new_struct, content)

# 2. Re-write handlePreloadKnowledge
handler_regex = re.compile(r'// Actually preload data[\s\S]*?if err := trans\.Commit')

new_handler = """// Actually preload data
var items []ai.Item[map[string]any]

if len(req.CustomData) > 0 {
// 1) From Custom Data (UI Upload)
var chunks []struct {
ID          string `json:"id"`
Category    string `json:"category"`
TexTexTexTexTexext"`
DescDescDescDescDescDescDescDescDescDarDescDescta, &chDescDescD{
mt.Sprintf("custom_%d", len(items)) }
textIndexStr :=textIndexStr :=textIndexStr :=textIndexx != nil { textIdx.Add(ctx, cid, textIndexStr) }
items = append(items, ai.Item[map[string]any]{
ID: cid,
Payload: map[string]any{
"text":        chunk.Text,
"description""description""description""description""origina"description""description" else if req.URL != "" {
// 2) From Internet URL Fetch
reqHTTP, err := http.NewRequestWithContext(ctx, http.MethodGet, req.URL, nil)
if err == nil {
resp, err := http.DefaultClient.Do(reqHTTP)
if err == nif err == nif err == nseif err == nif err == nif eD if err == nifon:"id"`
Category    string `json:"category"`
Text        string `json_Text        string `n string `jText        string `jsoText        string `n string `jText        string `json_Text        stnkText        string `jsoText        string `n string `jText        string `json_Text        textIndeText        string `jsoText        string `n string `jText        string `json_Text        storelist.txt items = append(items, ai.Item[map[string]any]{
Icid,
k.Text,
"description": chunk.Description,
"category":    chunk.Category,
"original_id": cid,
},
})
}
}
}
}
} els} els} els} els} els} elsso} els} els} els} els} els} elsso} els} els} els} els} els} elsso}rror
pathsToTry :=pathsToTry :=pathsToTry :=pathsToTry :=/"pathsToTry :=pathsToTry :=pathsToTry :=pathsToTry :=/"pathsToTry :=pathsToTry :=pathsToT
}
papppppppppprr := json.Unmarshal(fileBytespcifm) }
 ++ churi
iftextIdx iftextIdx iftextIdx iftextIdx iftextIdx iftextIdx iftextIdx iftextIdx ifPiftextIdx iftextIdx iftextIdx iftextIdx iftextIdx iftextIdx iftextIdx iftextIdx ifPiftextIdx ifid,
},
})
}
}
} else {
trans.Rollback(ctx)
http.Error(w, "Failed to find Knowledge Base file locally or provided data", http.StatusInternalServerError)
return
}
}

if len(items) > 0 {
vs.UpsertBatch(ctx, items)
}

if err := trans.Commit"""

content = handler_regex.sub(new_handler, content)

with open("tools/httpserver/knowledge_manager.go", "w") as f:
    f.write(content)

print("Go Code Patched!")
