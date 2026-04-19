import re

with open("templates/scripts_part11.html", "r") as f:
    text = f.read()

text = re.sub(
    r"\{\s*method:\s*'POST',\s*body:\s*JSON\.stringify\(([\s\S]*?)\)\s*\}",
    r"'POST', \1",
    text
)

with open("templates/scripts_part11.html", "w") as f:
    f.write(text)

