with open("store.go", "r") as f:
    text = f.read()

upsert_pos = text.find("\n\t\t// 3. Insert into items tree")

update_logic = """
t
dC, _ := s.categories.Find(ctx, bestCategory, false)
dC {
s.categories.GetCurrentValue(ctx)
t++
tItem(ctx, bestCategory, c)
ew_text = text[:upsert_pos] + update_logic + text[upsert_pos:]

with open("store.go", "w") as f:
    f.write(new_text)
