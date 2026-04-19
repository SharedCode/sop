import re
import os

with open('/Users/grecinto/sop/tools/httpserver/templates/modals.html', 'r') as f:
    text = f.read()

# Add download internet URL to the dropdown
add_internet_opt = '<option value="custom">Upload Custom JSON...</option>\n                        <option value="internet">Download from Internet URL...</option>'
text = text.replace('<option value="custom">Upload Custom JSON...</option>', add_internet_opt)

# Add internet URL input container
internet_input = '''<div id="new-kb-internet-container" style="display: none; margin-top:10px;">
                        <input type="text" id="new-kb-url" placeholder="https://example.com/dataset.json" style="width: 100%; padding: 8px; border-radius: 4px; border: 1px solid #ccc; margin-bottom:10px;">
                        <small style="color: #666; display:block; margin-bottom:10px;">Provide a direct URL to a JSON array containing the schema (id, category, text, description).</small>
                    </div>'''
text = re.sub(r'(<div id="new-kb-custotext = rner"[\s\S]*?<text = re.sub(r'(<div id="new-kb-custotext = rner"[\s\S]*?<text = re.sub(r'(<di/grecinto/sop/tools/httpstext = re.sub(r'(<div id="new-kb-custotex   text = re.sub(r'(<div id="new-kb-custotext = rner"[\s\S]*?<text = re.sub(r'(<div id="new-kb-custotext = rner"[\s\S]*?<text = re.sub(r'(<di/grecinto/sop/tools/hshow/text = re.sub(r'(<div id="new-kb-custotext = rner"[\s\S]*?<text = re.sub(r'(<div id="newd('new-ktext = re.sub(r'(<div id="new-kb-nttext = re.sub(r'(<div id="new-kb-custotext = rner"[\s\S]*?<text = re.sub(r'(<div id="new-kbe === text = re.sub(r'(<div id="new-kb-custotext = rner"[\s\S]*?<text = re.sub(r'(<div id="new-kb-custotext = rner"[\s\S]*?<text = re.sub(r'(<di/grecinto/sop/tools/httpstext = re.sub(r'(<div id="new-kb-custotex  text = re.sub(r'(<div id="new-kb-custotext = rner"[\s\S]*?<text = re.sub(r'(<div id="new-kb-custotext = rner"[\s\S]*?<text = re.sub(r'(<di/grecinto/sop/tools/httpstext = re.sub(r'(<div idst customContainer = document\.getElementById\(\'new-kb-custom-container\'\);\s*if \(template === "custom"\) \{\s*customContainer\.style\.display = "block";\s*\} else \{\s*customContainer\.style\.display = "none";\s*\}', sh_logic, stext)

# Update submit block
submit_logic = '''
    } else if (template === "internet") {
        const urlStr = document.getElementById('new-kb-url').value.trim();
        if (!urlStr) {
            showAlert("Please enter a valid dataset URL.");
            return;
        }
        showAlert("Wait while downloading and ingesting the Knowledge Base from the internet...", false, 3000);
        const expertiseData = {
            expertise_id: kbName,
            knowledge_base_name: kbName,
            database_name: currentDatabase,
            blob_store_base_folder_path: ecTarget,
            url: urlStr
        };
        apiCall('/api/knowledge/preload', {
            method: 'POST',
            body: JSON.stringify(expertiseData)
        }).then(() => {
            showAlert("Dataset Successfully Downloade            showAlert("Dataset Successfully Downloade            showAlert("Dataset Successfully Downloade            showAlert("Dataset Successfully Downloade            showAlert("Dataset Successfully D !=            showAlerxt            show('}            showAlert("Dataset Succesubmit            showAlert("Dataset Successfully Downloade            showAlpts_            showAl as f:
    f.write(stext)
