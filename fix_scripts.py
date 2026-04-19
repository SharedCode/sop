import re

with open('/Users/grecinto/sop/tools/httpserver/templates/scripts_part01.html', 'r') as f:
    content = f.read()

# Fetch knowledge bases after stores
kb_fetch = """        // Fetch KBs
        fetch(`/api/knowledge-bases?database=${encodeURIComponent(name)}`)
            .then(res => res.json())
            .then(kbs => {
                const list = document.getElementById('kb-list');
                list.innerHTML = '';
                if (!kbs || kbs.length === 0) {
                    list.innerHTML = '<li>No knowledge bases found</li>';
                    return;
                }
                kbs.forEach(kb => {
                    const li = document.createElement('li');
                    li.textContent = kb.charAt(0).toUpperCase() + kb.slice(1);
                    li.onclick = () => selectKnowledgeBase(kb, li);
                    list.appendChild(li);
                });
            })
            .catch(err => {
                const list = document                const list =                              const list = document                const list =                      
"""

# Insert after fetchin# Insert after fetchin# Insert afplac# Insert after fetchin# I?database=${encodeURIComponent(name)}`)", kb_fetch + "\n        fetch(`/api/db/option# Insert after fetchin# Insert aftee)}`)")

with open('/Users/grecinto/sop/tools/httpserver/templates/scripts_part01.html', 'w') as f:
    f.write(content)
