html = open("tools/httpserver/templates/scripts_part01.html").read()
import re
js = re.search(r"<script>(.*?)</script>", html, re.DOTALL).group(1)
try:
    compile(js, "scripts.js", "exec")
except Exception as e:
    print("Syntax Error!")
    print(e)
