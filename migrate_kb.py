import re

with open('tools/httpserver/templates/index.html', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. wipe out knowledge dashboard
text = re.sub(r'<!-- Knowledge Base Page -->\s*<div id="knowledge-page".*?</div>\s*</div>', '', text, flags=re.DOTALL)

# 2. insert add-kb-modal before script modal
kb_html = """
<!-- Add Knowledge Base Modal -->
<div id="add-kb-modal" class="modal">
    <div class="modal-content" style="width: 400px;">
        <div class="modal-header">
            <span>Add Knowledge Base</span>
            <span style="cursor:pointer; font-size: 20px;" onclick="closeAddKBModal()">&times;</span>
        </div>
        <div class="modal-body">
            <div style="margin-bottom: 15px;">
                <label style="display:block; margin-bottom:5px;">Knowledge Base Name:</label>
                <input type="text" id="new-kb-name" class="form-input" style="width: 100%; box-sizing: border-box;" placeholder="e.g. My Domain KB">
            </div            </div            </n-bottom: 15px;">            </div            </div            </n-bottom: 15px;">            <ab            </div   <            </div            </ds=            </div            </div            </n-bottom: 15px;">            </div            </div            </n-bo          <option value="sop_framework">Pre-load SOP Framework</option>
                    <option value="medical">Pre-load Medical E                    <option value="medical">Pre      <                    <option vadiv class="modal-footer">
            <button onclick="closeAddKBModal()" class="btn-secondary">Cancel</button>
            <button onclick="saveNewKB(           tn-s            <button onclick="sa <            <button onclick="saveNewKB(  b-moda            <button onclick="saveNewKB(           tn-s            <button onclick="sa <-- Scri            <button onclick="saveNewKB(           tn-s            <button onclick="sa <            <button onclick="saveNewKB(  b-moda         essfully updated.")
