import re

with open("templates/scripts_part11.html", "r") as f:
    text = f.read()

# Replace custom upload apiCall
text = re.sub(
    r"apiCall\('/api/knowledge/preload',\s*\{\s*method:\s*'POST',\s*body:\s*JSON\.stringify\(expertiseData\)\s*\}\)",
    r"apiCall('/api/knowledge/preload', 'POST', expertiseData)",
    text
)

# Replace internet URL apiCall
url_payload = r"""\{
                expertise_id: kbName,
                database_name: currentDatabase,
                knowledge_base_name: kbName,
                url: urlStr,
                blob_store_base_folder_path: ecTarget
            \}"""
text = re.sub(
    r"apiCall\('/api/knowledge/preload',\s*\{\s*method:\s*'POST',\s*body:\s*JSON\.stringify\(" + url_payload + r"\)\s*\}\)",
    r"apiCall('/api/knowledge/preload', 'POST', { expertise_id: kbName, database_name: currentDatabase, knowledge_base_name: kbName, url: urlStr, blob_store_base_folder_path: ecTarget })",
    text
)


with open("templates/scripts_part11.htmwith open("templates/scripts_xt)

