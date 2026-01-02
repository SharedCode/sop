import pytest
from playwright.sync_api import Page, expect
import time
import subprocess
import os
import requests

# Server configuration
SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"


def test_edit_empty_store_freedom(page: Page, sop_server):
    # Listen for console logs
    page.on("console", lambda msg: print(f"BROWSER CONSOLE: {msg.text}"))
    
    # 1. Create a new empty store via API
    test_store_name = f"empty_store_{int(time.time())}"
    
    payload = {
        "database": "TestDB",
        "store": test_store_name,
        "key_type": "string",
        "value_type": "string",
        "description": "Initial Description",
        "seed_key": None, # Ensure it's empty
        "seed_value": None
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
    
    # 5. Verify Structural Fields are ENABLED
    key_type = page.locator("#new-store-key-type")
    expect(key_type).to_be_enabled()
    
    value_type = page.locator("#new-store-value-type")
    expect(value_type).to_be_enabled()
    
    # Slot Length should be ENABLED (Editable for empty store)
    slot_length = page.locator("#new-store-slot-length")
    expect(slot_length).to_be_enabled()

    # IsUnique should be ENABLED (Editable for empty store)
    is_unique = page.locator("#new-store-is-unique")
    expect(is_unique).to_be_enabled()

    # Data Size should be ENABLED (Editable for empty store)
    data_size = page.locator("#new-store-data-size")
    expect(data_size).to_be_enabled()
    
    # 6. Verify Unlock Button is HIDDEN (already unlocked for empty store)
    unlock_btn = page.locator("#btn-unlock-admin")
    expect(unlock_btn).not_to_be_visible()
    
    # 7. Verify Description and Cache Duration are ENABLED
    desc = page.locator("#new-store-description")
    expect(desc).to_be_enabled()
    
    cache = page.locator("#new-store-cache-duration")
    expect(cache).to_be_enabled()
    
    # 8. Make Structural Changes
    # Change Key Type to Int
    key_type.select_option("int")
    
    # Provide Seed Data to persist the type (since StoreInfo only stores IsPrimitiveKey)
    # If we don't provide data, an empty primitive store defaults to "string" in UI.
    page.fill("#new-store-seed-key", "123")
    page.fill("#new-store-seed-value-simple", "val")

    # Change Description
    desc.fill("Updated Description")
    
    # 9. Submit Update
    save_btn = page.locator("#add-store-modal .modal-footer .btn-success")
    save_btn.click()
    
    # 10. Verify success
    expect(page.locator("#add-store-modal")).to_be_hidden()
    
    # 11. Verify changes persisted
    # Wait a bit
    page.wait_for_timeout(1000)
    edit_btn.click()
    expect(page.locator("#add-store-modal")).to_be_visible()
    
    expect(key_type).to_have_value("int")
    expect(desc).to_have_value("Updated Description")
    
    print("Test passed!")
