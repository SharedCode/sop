import pytest
from playwright.sync_api import Page, expect
import time
import subprocess
import os
import requests

# Server configuration
SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"


def test_cel_readonly_when_index_present_and_populated(page: Page, sop_server):
    page.on("console", lambda msg: print(f"BROWSER CONSOLE: {msg.text}"))

    test_store_name = f"cel_locked_{int(time.time())}"

    # Create store with Index Spec but NO CEL
    payload = {
        "database": "TestDB",
        "store": test_store_name,
        "key_type": "map",
        "value_type": "string",
        "description": "Store with Index but no CEL",
        "index_spec": '{"index_fields":[{"field_name":"id","ascending_sort_order":true}]}',
        "cel_expression": "",
        "seed_key": {"id": 1, "name": "seed"},
        "seed_value": "value1",
    }

    print(f"Creating store {test_store_name}...")
    response = requests.post(f"{SERVER_URL}/api/store/add", json=payload)
    assert response.status_code == 200, f"Failed to create store: {response.text}"

    # Ensure count > 0 (add another item)
    item_payload = {
        "database": "Python Complex DB",
        "store": test_store_name,
        "key": {"id": 2, "name": "test2"},
        "value": "value2",
    }
    requests.post(f"{SERVER_URL}/api/store/item/add", json=item_payload)

    page.goto(SERVER_URL)
    expect(page).to_have_title("SOP Data Manager")

    store_item = page.locator(f"#store-list li:has-text('{test_store_name}')")
    expect(store_item).to_be_visible(timeout=10000)
    store_item.click()

    edit_btn = page.locator("#btn-edit-store")
    expect(edit_btn).to_be_visible()
    edit_btn.click()

    expect(page.locator("#add-store-modal")).to_be_visible()

    # CEL must be read-only because Index exists and store is populated
    cel_edit_btn = page.locator("#new-store-cel-expression button")
    expect(cel_edit_btn).to_be_disabled()

    cel_value = page.locator("#cel-expression-value")
    expect(cel_value).to_be_disabled()

    print("Test passed!")
