import pytest
from playwright.sync_api import Page, expect
import time
import subprocess
import os
import requests

# Server configuration
SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"


def test_unlock_hidden_for_empty_store(page: Page, sop_server):
    """
    Verifies that the 'Unlock Admin' button is HIDDEN for an empty store,
    even if it has no Index/CEL (because it's already editable).
    """
    # 1. Create an empty store via API
    test_store_name = f"empty_unlock_test_{int(time.time())}"
    
    payload = {
        "database": "TestDB",
        "store": test_store_name,
        "key_type": "string",
        "value_type": "string",
        "description": "Empty store for unlock test",
        # No seed data -> Empty store
    }
    
    print(f"Creating store {test_store_name}...")
    response = requests.post(f"{SERVER_URL}/api/store/add", json=payload)
    assert response.status_code == 200, f"Failed to create store: {response.text}"
    
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

    # 4. Verify Unlock Button is HIDDEN
    unlock_btn = page.locator("#btn-unlock-admin")
    expect(unlock_btn).not_to_be_visible()
    
    # 5. Verify fields are still editable (e.g. Key Type)
    # Key Type should be enabled for empty store
    expect(page.locator("#new-store-key-type")).to_be_enabled()
    
    print("Test passed!")

def test_unlock_hidden_for_empty_store_with_index(page: Page, sop_server):
    """
    Verifies that the 'Unlock Admin' button is HIDDEN for an empty store
    that HAS an Index Spec. It should still be editable because it's empty.
    """
    
    # 1. Create an empty store WITH Index Spec
    test_store_name = f"empty_index_test_{int(time.time())}"
    
    payload = {
        "database": "TestDB",
        "store": test_store_name,
        "key_type": "map",
        "value_type": "string",
        "description": "Empty store with index",
        "index_spec": '{"index_fields":[{"field_name":"id","ascending_sort_order":true}]}',
        "seed_key": {"id": 1, "name": "seed"},
        # No seed data -> Empty store (Wait, seed_key implies seed data? No, seed_key is just schema hint if not added)
        # Actually, the API might add the seed item if seed_key/value are present.
        # Let's ensure we don't add an item.
        # The /api/store/add endpoint adds the seed item if seed_key/value are provided.
        # To create a truly empty store with schema, we might need to delete the item or use a different flow.
        # But wait, if count > 0, it's not empty.
        # If we want an empty store with Index Spec, we can create it, then delete the item.
    }
    
    # Create store (will have 1 item due to seed)
    requests.post(f"{SERVER_URL}/api/store/add", json=payload)
    
    # Delete the item to make it empty
    requests.post(f"{SERVER_URL}/api/store/item/delete", json={
        "database": "TestDB",
        "store": test_store_name,
        "key": {"id": 1, "name": "seed"}
    })
    
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
    
    # 4. Verify Unlock Button is HIDDEN (because it's empty, so editable)
    unlock_btn = page.locator("#btn-unlock-admin")
    expect(unlock_btn).not_to_be_visible()
    
    # 5. Verify Index Fields are Editable
    add_index_btn = page.locator("button[onclick='addIndexFieldRow()']")
    expect(add_index_btn).to_be_enabled()
    
    print("Test passed!")
