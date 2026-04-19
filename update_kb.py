import sys

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


# 2. Extract block
idx_start = data.find('if (template !== "empty") {')
if idx_start == -1:
    print("could not find start")
    sys.exit(1)

idx_end = data.find('} else {', idx_start)
if idx_end == -1:
    print("could not find end")
    sys.exit(1)

original_block = data[idx_start:idx_end]

new_block = """if (tenew_block = """if (tenew_bl    const filenew_block = """if (tenew_block = """if (tenew_bl    const filenew_bleInput.files.length === 0) {
            showAlert("Please select a JSON file to upload.");
                                                                                                                                                                                              pa                                                    const expe                                        ise_id: kbName,
                    knowledge_base_name: kbName,
                    database_name: currentDatabase,
                                                                                         a: pars                                                                    d',                                                           body: JSON.st                                         .t                                           Custom       dge Base cr                                                                                                                                   a: pars                                                                                                                            ng JSON file. Please ensure it is valid JSON.");
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
print("Done")
