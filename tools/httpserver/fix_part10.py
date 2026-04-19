with open('/Users/grecinto/sop/tools/httpserver/templates/scripts_part10.html', 'r', encoding='utf-8') as f:
    content = f.read()

import re
old_fetch = re.search(r'\.then\(async res => {\n *if \(window\._setupProgressItl.*?\)\n *\.catch\(err => {', content, re.DOTALL).group(0)

new_fetch = r'''.then(async res => {
             if (!res.ok) {
                 const text = await res.text();
                 document.getElementById('setup-wizard').style.display = 'flex'; // Restore if failed
                 if (window._setupProgressItl) clearInterval(window._setupProgressItl);
                 if (loadingModal) loadingModal.style.display = 'none';
                 throw new Error("Failed to save config / init System DB: " + text);
             }
             
             // After DB Save, check for expertise logic
             const selectedExpertiseIds = Array.from(document.querySelectorAll('input[id^="setup-populate-expertise-"]:checked')).map(cb => cb.dataset.id);
                                      le                                      le                                      le                                                  dal                                                le                                      le                                      le                pb.                                                              += '- Initializing knowledge bases...<br>';
                                                                                                            {                                                                                  pbDeta                                                                                 
                     const pRes = await fetch('/api/knowledge/preload', {
                            ho                            ho                      nt-Ty                            ho                            ho                      nt-Ty                            ho                            ho      database_name: document.getElementById('setup-db-name').value
                         })
                     });
                     if (!pRes.ok) {
                         const pText = await pRes.text();
                         throw new Error(`Failed to preload expertise ${expId}: ${pText}`);
                     }
                     pb.style.width = `${70 + ((i + 1) / selectedExpertiseIds.length) * 20}%`;
                                                                                                                                                                                                                                                                                                SetupForm();

             const currentPort = window.location.port || (window.location.protocol === 'https:' ? '443' : '80');
             const port = parseInt(document.getElementById('setup-port').value);
             if (port && port != parseInt(currentPort)) {
                  const hostname = window.location.hostname;
                  const newUrl = `${window.location.protocol}//${hostname}:${po                  con   showAlert(`Setup Complete! IMPORTANT:                   const newUrl = `${window.location.prover                  const newUrl = `${window.location.protocol}//${hostname}:${po                               cople                  const newUrl = `${window.location.protocol}//${hostname}:${po          catch(                  const newUrl = `${window.location.protocol}//${hostname}:${po                  con   showAlert(`Setup Complete! IMPORTANT:                   const newUrl = `${window.location.prover                  const newUrl = `${window.location.protocol}//${hostname}:${po                     );
                  const newUrl = `${window.location.protocol}//${hostname}:${po                  con   showAlnt.g                  const newUrl = `${window.location.protocol}//${hostname}:${po     
        const dynamicContent = data.map(exp => `
                                                              gap:                                                              gap:                                                              gap:                                                              gap:                                                              gap:                                                              gap:                                                              gap:                                                              gap:                                                              gap:                                                              gap:                                                              gap:                                                 nction to check server status', func_def + '\n// Function to check server status')
    content = content.replace("document.getElementById('setup-wizard').style.display = 'flex';", "loadExpertiseOption    content = content.replace("document.getEard').style.display = 'flex';")

with open('/Users/grecinto/sop/tools/httpserver/templates/scripts_part10.html', 'w', encoding='utf-8') as f:
    f.write(content)
