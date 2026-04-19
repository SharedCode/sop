import re

with open('tools/httpserver/templates/scripts_part11.html', 'r') as f:
    data = f.read()

toggle_script = """function toggleCustomUpload() {
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

# Inject before saveNewKB
data = data.replace('function saveNewKB() {', toggle_script + '\nfunction saveNewKB() {')

save_logic_old = """    if (template !== "empty") {
        // Use the existing preload endpoint 
        const expertiseData = {
            expertise_id: template,
            database_name: currentDatabase,
            blob_store_base_folder_path: ecTarget
            // we should technically pass kbName to the preload or rename the kb aft            // we should technipe  s _knowle            // we should technically pass kbName to the preload or re               'POST',
            // we should technically pass kbNam
                                                                    ded su                                                                    ded su          s                             ledg                                               e            logic_new = """    if (template === "custom") {
        const fileInput = document.getElementById('custom-json-fil    
                                th === 0) {
            showAlert("Please select a JSON file to upload.");
            return            return            return            re];            return            return  ();
                                  )                    
                const parsed = JSON.parse(e.target.result);
                const expertiseData = {
                                                                                                                                                                                                                                    _data: parsed
                };
                a                a                a                a                a                a                a                a                a                a                a                a                a                a                a                a                a                a         ch                a                a                a                a e:                a                  });
            } catch (err) {
                showAlert("Error parsing                showAlert("Error parsing                showAlert("Error parsing                showAlert("Err e                showAlert("Error parsing       the exist                showAlert("Error parsing                          expertise_id: template,
                                                                                                                                                                                                                                                          (e                                                                                                                                                                                                                                                                             (e                                                                                                                          ite(data)
