import os
filepath = "templates/scripts_part01.html"
with open(filepath, "r") as f:
    text = f.read()

orig_stores = "                // Remove filter that prevents vectorDBs to be displayed\n                stores.forEach(store => {"
new_stores = "                stores.forEach(store => {"
text = text.replace(orig_stores, new_stores)

with open(filepath, "w") as f:
    f.write(text)

print("done script fix for part 1")
