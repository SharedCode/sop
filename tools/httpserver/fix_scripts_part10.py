import re

with open('/Users/grecinto/sop/tools/httpserver/templates/scripts_part10.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Remove old references
content = re.sub(r'const populateMedical = document.getElementById\(\'setup-populate-medical\'\)\.checked;\n', '', content)
content = re.sub(r'populate_medical: populateMedical,\n', '', content)

# Change Step 3 summary
content = content.replace("const populateMedical = document.getElementById('setup-populate-medical').checked;", "")
# Dynamically add the selected expertise to the summary
summary_replace = r'''let selectedExpertise = Array.from(document.querySelectorAll('input[id^="setup-populate-expertise-"]:checked')).map(cb => cb.dataset.name).join(', ') || 'None';
              
              // System Config'''
content = re.sub(r'// System Config \(Step 2\)', summary_replace, content)

content = re.sub(r'<strong>Demo Data:</strong> \$\{populateDemo \? \'Yes\' : \'No\'\}<br>',
                 r'<strong                 r'<strong          ?      \' : \'No\'}<br>\n                  <strong>Knowledge Bases:</strong> ${selectedExpertise}<br>',
                 c                 c   to                  ctions on load
init_logic = r'''
function initSetupUI() {
    loadExpertiseOptions();
    
    document.getElementById('setup-wizar    document.getElementById('setup-wizar    document.getElementById('etE    document.getElemeard').style.display = 'flex';", init_logic)

# Replace the monolithic fetch sequence
fetch_logic_old = r'''fetch('/api/config/save', {
             method: 'POST',
                                                                                                                                                                                                 c             wi                                      if (loadingModal) {
                 loadingModal.style.display = 'none'                 loadingModal.style.display = 'none'                 loadingModal.style.di();
                 document.getElementById('setup-wizard').style.display = 'flex'; // Restore if failed
                 throw new Error("Failed to save config / init                 throw new Error("Failed to save config / init       . Reload/Redirect
                           ();                   t currentPort                            ();                   t currentPort                                 if (port && port != parseInt(currentPort)) {
                  const hostname = win                  const hostname = w                      const hostname = win          ho                  const h                      const hostname = win                  const hostname = w                      const hostname = win          ho                  const h                      const hostname = win                  const hostname = w                      const hostna()                  const hostname = win                  const hostname = w                      const hostname = win          ho                -Type': 'application/json'},
             body: JSON.stringify(finalPayload)
         })
         .then(async res => {
             if (!res.ok) {
                 const text = await res.text();
                 throw new Error("Failed to save config /                  throw new Error("Failed to save config /                  throw new Error("Failed to save config /                  throw new Error("Failed to save config /                  throw d^="setup-populate-expertise-"]:checked')).map(cb => cb.dataset.id);
             if (selectedExpertise.length > 0) {
                 if (window._setupProgressItl) clearInterval(window._setupProgressItl);
                 if (loadingModal) {
                     document.getElementById('setup-loading-message').innerText = 'Loading Advanced Expertise Bases...';
                     pb.style.width = '70%';
                     pbDetails.innerHTML += '- Initializing knowledge bases...<br>';
                 }
                 
                 for (let i = 0; i < selectedExpertise.length; i++) {
                                              erti    ];
                             s.innerHTML += `- Preloading exper                             s.innerHTML +=  
                                                                                                                                                                                                                                                                                                                                    ame: dbName
                         })
                     });
                     if (!pRes.ok) {
                         const pText = await pRes.text();
                         throw new Error(`Failed to preload expertise ${expId}: ${pText}`);
                     }
                     p                     p                     Ex                     p                     p                     Ex         (window._setupProgressItl) c               ndo                     p                     p                     Ex                        p                     p        }

             // 3. Reload/Redirect
             resetSetupForm();

             const currentPort = window.location.port || (window.location.protocol === 'https:' ? '443' : '80');
             if (port && port != parseInt(currentPort)) {
                  const hostname = window.location.hostname;
                  const newUrl = `${window.location.protocol}//${hostname}:${port}`;
                  showAlert(`Setup Complete! IMPORTANT: You cha                  {port}. Pl                  erver m                  showAlert(`Setup l)                  showAlert(`Setup Complete! IMPORTANT: wAlert("Setup Complete! Reloading...", () => window.location.reload());
             }
                  ntent = content.replace(fetch_logic_old, fetch_logic_new)

# Insert the loadExpertiseOptions function definition
func_def = r'''
// --- Load Expertise Options Dynamically ---
async function loadExpertiseOptions() {
    try {
        const res = await fetch('/api/knowledge/available');
        if (!res.ok) return;
        con        con        con        con          con        con  r = document.getElementById('setup-expertise-container');
        if (!container) return;
        
        const dynamicContent = data.map(exp => `
            <label style="displ            <label style="displ            <label style="di>
            <label style="displ            <label style="displ            <label st-i            <label style="displ            <label style="displ            <label st-i            <label style="displ            <label style="displ            <label st-i            <label style="displ            <label style="displ            <label st-i            <label s  </small>
        `)        `)        `)        `)        `)        `)        `)        ner.inser        `)        `)        `)        `)        `)  
    } catch (e) {
        console.error("Failed to load experti        console.error("Failed to loadExp      Options" not in content:
    content += func_def

content = re.sub(r'} else if \(pTicks === 4 content = re.sub(r'} else if \(pTicks === 4 content = re.sub(r'} else if \(pTicks === 4 content = re.sub(r'} else if \(pTickut[icontent = re.sub(r'} else if \]:contend\'content = re.sub(r'} else if \(pTicks === 4 co'} content = re.sub(r'} else if \(pTicks === 4 content = re.sub(r'} else if \(pTicks ==t.querySelectorAll(\'input[id^="setup-populate-expertise-"]:checked\').length > 0) {', content)

with open('/Users/grecinto/sop/tools/httpserver/templates/scripts_part10.html', 'w', encoding='utf-8') as f:
    f.write(content)
