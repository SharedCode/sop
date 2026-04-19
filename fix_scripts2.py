import re

with open('/Users/grecinto/sop/tools/httpserver/templates/scripts_part01.html', 'r') as f:
    content = f.read()

select_kb = """    async function selectKnowledgeBase(name, element) {
        currentStore = name + "/data";
        
        closeUIModal('toolbar');
        closeUIModal('pagination');
        closeUIModal('pagination-top');
        
        if (!await closeDetail()) return;

        columnWidths = [];
        columnsInitialized = false;

        document.querySelectorAll('#sidebar li').forEach(el => el.classList.remove('active'));
        element.classList.add('active');

        document.getElementById('current-store-title').textContent = "KB: " + name;
        
        const grid = document.getElementById('data-grid');
        grid.innerHTML = "Fetching knowledge base thoughts...";
        
        try {
            const res = await fetch(`/api/knowledge/thoughts?database=${encodeURIComponent(currentDatabase)}&name=${encodeURIComponent(name)}`);
                               ai        on(                               ai        on(                               ai         grid.                  No thoughts found in this Knowledge Base.</div>";
                return;
            }
            
            let html = `  iv style="display: flex; flex-wrap: wrap; gap: 15px; padding: 15px;            let html = `  iv stch(t => {
                                           div styl                       r(--border-color); border-radius: 8px; pa                                           div styl                       r(--boiv style="font-                                    ); margin-bottom: 8px;">Category: ${t.category}</div>
                    <div style="font-size: 1.                    <div style="font-size: 1.           div                    <div style="font-size: 1.                 (-   xt-mut  );">${t.desc                    <div style="font-size: 1.                    <div style="font-size: 1.           div                    <div style="fon     <                    <div style="font-size: 1.                    <div style="font-size: 1.           div                    <div style="font-size: 1.                 (-   xt-mut  );">${t.desc           $                >`;
        }
    }
"""

content = content.replace("async function selectStore(name, element, force = false) {", select_kb + "\n    async function selectStore(name, element, force = false) {")

with open('/Users/grecinto/sop/tools/httpserver/templates/scripts_part01.html', 'w') as f:
    f.write(content)
