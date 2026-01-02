import pytest
from playwright.sync_api import Page, expect
import time
import subprocess
import os
import requests

# Server configuration
SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"


def test_lock_populated_store(page: Page, sop_server):
    # Listen for console logs
    page.on("console", lambda msg: print(f"BROWSER CONSOLE: {msg.text}"))
    
    # 1. Create a store WITH Index Spec and CEL
    test_store_name = f"locked_store_{int(time.time())}"
    
    payload = {
        "database": "TestDB",
        "store": test_store_name,
        "key_type": "map",
        "value_type": "string",
        "description": "Store with Index and CEL",
        "index_spec": '{"index_fields":[{"field_name":"id","ascending_sort_order":true}]}',
            "cel_expression": "mapX.id < mapY.id ? -1 : (mapX.id > mapY.id ? 1 : 0)",
    }
    
    print(f"Creating store {test_store_name}...")
    response = requests.post(f"{SERVER_URL}/api/store/add", json=payload)
    assert response.status_code == 200, f"Failed to create store: {response.text}"
    
    # 2. Populate with data (Count > 0)
    # The seed data in create might not count as "populated" depending on implementation, 
    # but usually it adds the first item. Let's verify count.
    
    # Add another item just to be sure
    item_payload = {
        "database": "TestDB",
        "store": test_store_name,
        "key": {"id": 2, "name": "test2"},
        "value": "value2"
    }
    requests.post(f"{SERVER_URL}/api/store/item/add", json=item_payload)

    # 3. Load the page
    page.goto(SERVER_URL)
    expect(page).to_have_title("SOP Data Manager")
    
    # 4. Select the store
    store_item = page.locator(f"#store-list li:has-text('{test_store_name}')")
    expect(store_item).to_be_visible(timeout=10000)
    store_item.click()
    
    # 5. Open Edit Modal
    edit_btn = page.locator("#btn-edit-store")
    expect(edit_btn).to_be_visible()
    edit_btn.click()
    
    expect(page.locator("#add-store-modal")).to_be_visible()
    
    # 6. Verify Structural Fields are DISABLED
    expect(page.locator("#new-store-slot-length")).to_be_disabled()
    expect(page.locator("#new-store-is-unique")).to_be_disabled()
    expect(page.locator("#new-store-data-size")).to_be_disabled()
    
    # 7. Verify Index Spec is DISABLED
    # The "Add Index Field" button should be disabled or hidden
    # Note: The UI might hide it or disable it. Let's check disabled state of existing rows.
    
    # Check existing index row inputs/selects
    index_selects = page.locator(".index-field-row select")
    count = index_selects.count()
    for i in range(count):
        expect(index_selects.nth(i)).to_be_disabled()
        
    # Check Add Button
    add_index_btn = page.locator("button[onclick='addIndexFieldRow()']")
    if add_index_btn.is_visible():
        expect(add_index_btn).to_be_disabled()

    # 8. Verify CEL Expression is DISABLED
    cel_input = page.locator("#cel-expression-value")
    expect(cel_input).to_be_disabled()
    
    # 9. Verify Unlock Button is HIDDEN (because it has specs)
    unlock_btn = page.locator("#btn-unlock-admin")
    expect(unlock_btn).to_be_hidden()
    
    print("Test passed!")
