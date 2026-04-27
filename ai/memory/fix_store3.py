import re

with open("/Users/grecinto/sop/ai/memory/store.go", "r") as f:
    text = f.read()

# 1. MemoryStore return
text = text.replace("return &store[T]{\n                categories: categories,\n                vectors:    vectors,\n                items:      items,\n                dedup:      true,\n        }", "s := &store[T]{\n                categories: categories,\n                vectors:    vectors,\n                items:      items,\n                dedup:      true,\n        }\n        return s")

# 2. item.Vector -> vec
text = text.replace("item.Vector", "vec")

# 3. strData typing error
text = text.replace('strData := item.ID + " " + fmt.Sprintf("%v", item.Data)', 'strData := item.ID.String() + " " + fmt.Sprintf("%v", item.Data)')

# 4. UpsertByCategory id Parsing error
text = text.replace("id, err := sop.ParseUUID(item.ID)", "id := item.ID; var err error = nil")

# 5. Fix UpsertByCategory vector
text = text.replace("ai.Vector{ItemID: id, Data: vec}", "ai.Vector{ID: vk.VectorID, ItemID: id, CategoryID: c.ID, Data: vec}")

with open("/Users/grecinto/sop/ai/memory/store.go", "w") as f:
    f.write(text)
