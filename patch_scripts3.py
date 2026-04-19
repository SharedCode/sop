import re

with open('tools/httpserver/templates/scripts_part11.html', 'r') as f:
    data = f.read()

# 1. toggleCustomUpload
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

# 2. Re-write the save block
idx_start = data.find('if (template !== "empty") {')
idx_end = data.find('} else {', idx_start)

if idx_start != -1 and idx_end != -1:
    new_block = """if (template === "custom") {
        const fileInput = document.getElementById('custom-json-file');
        if (fileInput.files.le        if (fileInput.files.le  ert("Please s        if (fileInput.files.le        if (fileInput.files.le  ert("Pl c        if (fileInput        if (fileInput.st        if (fileInput.files.le        if (fillo        if (fileInput.files.le        if (fileInput.files.le  ert("PlSO        if (fileInput.files.le        if (fileInput.files.le  ert("Please s                  if (fileInput.files.le           if (fileInput.files.le        if (fileI          if atabase_name: currentDatabase,
                    blob_store_base_folder_path: ecTarget,
                    custom_data: parsed
                };
                                                                                                                                (exper                                                                                               cr            sfully!");
                                                            tch(err => {
                    showAlert("Error creating Knowledge Base:                    showAlert("Error creating Knowledge Base:                    showAlert("Error creating Knowledge Base:                    showAlert("Error creating Knowledge Base:                    showAlert("Error creating Knowledge Base:                    showAlert("Error creating Knowledge Base:                    showAlert("Error creating Knowledge Base:                    showAlert("Error creating Knowledge Base:                    showAlert("Error creating Knowledge Base:                    showAler     method: 'POST',
            body: JSON.stringify(expertiseData)
        }).then(() => {
                     ("Know                     ("Know                     ("Know                     ("Know                     ("Know                     ("Know                     ("Know                     ("Know                     ("Know                     ("Know         en                     ("Know                     ("Know               rite(data)

print("Patch successful")
