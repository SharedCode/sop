with open('/Users/grecinto/sop/tools/httpserver/config_save_handler.go', 'r', encoding='utf-8') as f:
    text = f.read()

import re
old_chunk = re.search(r'if udb\.PopulateMedical {\n.*?log\.Info\(fmt\.Sprintf\("Medical knowledge base.*?\n.*?}\n.*?}', text, re.DOTALL).group(0)
text = text.replace(old_chunk, "")

with open('/Users/grecinto/sop/tools/httpserver/config_save_handler.go', 'w', encoding='utf-8') as f:
    f.write(text)
