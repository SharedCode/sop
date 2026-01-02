import pytest
from playwright.sync_api import Page, expect
import time
import subprocess
import os
import requests

# Server configuration
SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"


def test_unlock_flow_for_legacy_store(page: Page, sop_server):
    """
    Verifies the unlock flow for a non-empty store with missing Index/CEL.
    1. Fields should be DISABLED initially.
    2. Unlock button should be VISIBLE.
    3. Clicking Unlock and entering token should ENABLE fields.
    """
    # Listen for console logs
    page.on("console", lambda msg: print(f"BROWSER CONSOLE: {msg.text}"))
    page.on("pageerror", lambda exc: print(f"BROWSER ERROR: {exc}\nStack: {exc.stack}"))

    # 1. Create a store with Map key (so it's not primitive) and seed data (so it's not empty)
    # but NO index spec (simulating legacy store or one where index was removed? 
    # Actually, creating a map store usually requires index spec. 
    # But we can create it via API without index spec if the backend allows it, 
    # or create it with index spec and then maybe the UI treats it differently?
    # Wait, the UI logic checks `currentStoreInfo.indexSpec`.
    # If I create a store with map key, I must provide index spec usually.
    # Let's try to create one with empty index spec if possible.
    
    test_store_name = f"unlock_flow_test_{int(time.time())}"
    
    # We need a Map key type to avoid "Primitive Key" lock.
    # And we need it to be non-empty.
    # And we need it to have NO index spec (or empty index spec).
    
    payload = {
        "database": "TestDB",
        "store": test_store_name,
        "key_type": "map",
        "value_type": "string",
        "description": "Unlock flow test",
        # Minimal Index Spec to allow creation, but maybe we can trick it?
        # If I send empty index_fields, maybe?
        "index_spec": '{"index_fields": []}',
        # Seed data to make it non-empty
        "seed_key": {"id": 1},
        "seed_value": "val"
    }
    
    # Note: The backend might reject empty index spec for map store. 
    # But let's try. If it fails, we might need another way to simulate "missing index spec".
    # Or maybe the UI considers it missing if the list is empty.
    
    print(f"Creating store {test_store_name}...")
    response = requests.post(f"{SERVER_URL}/api/store/add", json=payload)
    
    # If creation fails because of missing index spec, we might need to create it WITH index spec
    # and then manually modify the store info in the backend? No, that's hard.
    # Let's assume the backend allows empty index spec for now, or that we can create it.
    
    if response.status_code != 200:
        # Fallback: Create with a dummy index, but maybe the UI logic 
        # `currentStoreInfo.indexSpec.index_fields.length > 0` will be true.
        # We need it to be false.
        print(f"Creation failed: {response.text}")
        pytest.fail("Could not create test store")

    # 2. Load page and select store
    page.goto(SERVER_URL)
    store_item = page.locator(f"#store-list li:has-text('{test_store_name}')")
    expect(store_item).to_be_visible(timeout=10000)
    store_item.click()

    # 3. Open Edit Modal
    edit_btn = page.locator("#btn-edit-store")
    expect(edit_btn).to_be_visible()
    edit_btn.click()

    expect(page.locator("#add-store-modal")).to_be_visible()

    # 4. Verify Initial State: LOCKED
    add_index_btn = page.locator('button[onclick="addIndexFieldRow()"]')
    expect(add_index_btn).to_be_disabled()
    
    cel_input = page.locator("#cel-expression-value")
    expect(cel_input).to_be_disabled()
    
    unlock_btn = page.locator("#btn-unlock-admin")
    expect(unlock_btn).to_be_visible()

    # 5. Perform Unlock
    # Setup dialog handler
    def handle_dialog(dialog):
        if dialog.type == "prompt":
            dialog.accept("secret_token")
        elif dialog.type == "alert":
            dialog.accept()
        else:
            print(f"Unexpected dialog type: {dialog.type}")
            dialog.dismiss()

    page.on("dialog", handle_dialog)
    
    unlock_btn.click()
    
    # 6. Verify Unlocked State
    expect(add_index_btn).to_be_enabled()
    expect(cel_input).to_be_enabled()
    expect(unlock_btn).not_to_be_visible()

    # 7. Verify Key/Value fields remain DISABLED (because store is non-empty)
    # Key Type
    expect(page.locator("#new-store-key-type")).to_be_disabled()
    expect(page.locator("#new-store-key-is-array")).to_be_disabled()
    
    # Value Type
    expect(page.locator("#new-store-value-type")).to_be_disabled()
    expect(page.locator("#new-store-value-is-array")).to_be_disabled()
    
    # Key Fields (if any exist, though we created a map store, so there might be rows if we added them in seed? 
    # No, seed key was {"id": 1}, but we didn't define key fields in the UI explicitly in the test setup.
    # The UI might auto-populate based on store info if it had them.
    # But let's check the "Add Key Field" button if it exists.
    add_key_btn = page.locator('button[onclick="addKeyFieldRow()"]')
    if add_key_btn.is_visible():
        expect(add_key_btn).to_be_disabled()

