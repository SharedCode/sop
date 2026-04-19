import os
filepath = "templates/scripts_part01.html"
with open(filepath, "r") as f:
    text = f.read()

orig_kbs = "                if (!kbs || kbs.length === 0) {\n                    kbList.innerHTML = '<li>No Playbooks found</li>';\n                    return;\n                }"
new_kbs = "                if (!kbs || kbs.length === 0) {\n                    kbList.innerHTML = '<li>No Playbooks found</li>';\n                    return;\n                }"
text = text.replace(orig_kbs, new_kbs)

with open(filepath, "w") as f:
    f.write(text)

print("done script fix for part 2 kbs")
