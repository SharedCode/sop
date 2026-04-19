import os
filepath = "templates/scripts_part01.html"
with open(filepath, "r") as f:
    text = f.read()

text = text.replace("// Fetch Knowledge Bases", "// Fetch Playbooks")

with open(filepath, "w") as f:
    f.write(text)

print("done renaming comments")
