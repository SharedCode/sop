import pytest
from playwright.sync_api import Page, expect
import time
import subprocess
import os
import json
import requests

# Server configuration
SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"

def test_fix_missing_spec_with_seed(page: Page, sop_server):
    # Listen for console logs
    page.on("console", lambda msg: print(f"BROWSER CONSOLE: {msg.text}"))
    
    # 1. Create a store via API without IndexSpec (simulating code-created store)
    test_store_name = f"fix_spec_seed_{int(time.time())}"
    
    payload = {
        "database": "TestDB",
        "store": test_store_name,
        "key_type": "map",
        "value_type": "string",
        "description": "Store without Index Spec",
        "index_spec": "", # Empty
        "cel_expression": "", # Empty
        "seed_key": {"id": 1, "name": "test"},
        "seed_value": "value1"
    }
    
    print(f"Creating store {test_store_name}...")
    response = requests.post(f"{SERVER_URL}/api/store/add", json=payload)
    assert response.status_code == 200, f"Failed to create store: {response.text}"
    
    # 2. Load the page
    page.goto(SERVER_URL)
    expect(page).to_have_title("SOP Data Manager")
    
    # 3. Select the store
    store_item = page.locator(f"#store-list li:has-text('{test_store_name}')")
    expect(store_item).to_be_visible(timeout=10000)
    store_item.click()
    
    # 4. Open Edit Modal
    edit_btn = page.locator("#btn-edit-store")
    expect(edit_btn).to_be_visible()
    edit_btn.click()
    
    expect(page.locator("#add-store-modal")).to_be_visible()

    # Unlock Admin to enable editing
    unlock_btn = page.locator("#btn-unlock-admin")
    expect(unlock_btn).to_be_visible()
    
    # Handle prompt
    def handle_dialog(dialog):
        if dialog.type == "prompt":
            dialog.accept("debug_token")
        else:
            dialog.accept()
            
    page.on("dialog", handle_dialog)
    unlock_btn.click()
    
    # 5. Add an Index Spec
    add_index_btn = page.locator("button[onclick='addIndexFieldRow()']")
    expect(add_index_btn).to_be_enabled()
    add_index_btn.click()
    
    select = page.locator(".index-field-select").first
    select.select_option("id")
    
    # 6. Submit Update (This will also send SeedValue)
    save_btn = page.locator("#add-store-modal .modal-footer .btn-success")
    save_btn.click()
    
    # 7. Verify success (Modal closes)
    expect(page.locator("#add-store-modal")).to_be_hidden()
    
    print("Test passed!")
