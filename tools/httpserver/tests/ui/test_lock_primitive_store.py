import pytest
from playwright.sync_api import Page, expect
import time
import subprocess
import os
import requests

# Server configuration
SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"


def test_unlock_hidden_for_primitive_store(page: Page, sop_server):
    """
    Verifies that the 'Unlock Admin' button is HIDDEN for a non-empty store
    with a PRIMITIVE key type, even if it has no Index/CEL.
    """
    # 1. Create a store with primitive key (string) and seed data (so it's not empty)
    test_store_name = f"primitive_lock_test_{int(time.time())}"
    
    payload = {
        "database": "TestDB",
        "store": test_store_name,
        "key_type": "string",
        "value_type": "string",
        "description": "Primitive store for lock test",
        "seed_key": "test_key",
        "seed_value": "test_value"
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
    # Current behavior (before fix): It might be visible because it has no Index/CEL.
    # Desired behavior: Hidden because it is primitive.
    unlock_btn = page.locator("#btn-unlock-admin")
    expect(unlock_btn).not_to_be_visible()
