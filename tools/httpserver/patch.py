import re

with open('tools/httpserver/templates/modals.html', 'r', encoding='utf-8') as f:
    modals = f.read()

modals = re.sub(r'<!-- Preferences Modal -->.+?</div>\n</div>\n', '', modals, flags=re.DOTALL)

with open('tools/httpserver/templates/modals.html', 'w', encoding='utf-8') as f:
    f.write(modals)
