import re
import sys

with open('tools/httpserver/templates/scripts_part11.html', 'r') as f:
    data = f.read()

toggle_fn = """function toggleCustomUpload() {
    const template = document.getElementById('new-kb-template').value;
    const uploadContainer = document.getElementById('custom-json-upload-container');
    if (template === 'custom') {
        uploadContainer.style.display = 'block';
    } else {
        uploadContainer.style.display = 'none';
        document.getElementById('custom-json-file').value = '';
    }
}
"""
if "toggleCustomUpload" not in data:
    data = data.replace('function saveNewKB() {', toggle_fn + '\nfunction saveNewKB() {')

idx_start = data.find('if (template !== "empty") {')
idx_end = data.find('} else {', idx_start)

if idx_start != -1 and idx_end != -1:
    new_block = """if (template === "custom") {
        const fileInput = document.getElementById('custom-json-file');
        if (fileInput.files.length === 0) {
            showAlert("Please select a JSON file to upload.");
            return;
        }
        const file = fileInput.files[0];
        const reader = new FileReader();
        reader.onload = function(e) {
            try {
                const parsed = JSON.parse(e.target.result);
                const expertiseData = {
                    expertise_id: kbName,
                    knowledge_base_name: kbName,
                    database_name: currentDatabase,
                    blob_store_base_folder_path: ecTarget,
                    custom_data: parsed
                };
                apiCall('/api/knowledge/preload', {
                    method: 'POST',
                    body: JSON.stringify(expertiseData)
                }).then(() => {
                    showAlert("Custom Knowledge Base created successfully!");
                    closeAddKBModal();
                }).catch(err => {
                    showAlert("Error creating Knowledge Base: " + err.message);
                });
            } catch (err) {
                showAlert("Error parsing JSON file. Please ensure it is valid JSON.");
            }
        };
        reader.readAsText(file);
    } else if (template !== "empty") {
        const expertiseData = {
            expertise_id: template,
            knowledge_base_name: kbName,
            database_name: currentDatabase,
            blob_store_base_folder_path: ecTarget
        };
        apiCall('/api/knowledge/preload', {
            method: 'POST',
            body: JSON.stringify(expertiseData)
        }).then(() => {
            showAlert("Knowledge Base preloaded successfully!");
            closeAddKBModal();
        }).catch(err => {
            showAlert("Error creating Knowledge Base: " + err.message);
        });
    """
    data = data[:idx_start] + new_block + data[idx_end:]
    with open('tools/httpserver/templates/scripts_part11.html', 'w') as f:
        f.write(data)
    print("Patch successful")
else:
    print("Could not find replacement block in scripts_part11")
