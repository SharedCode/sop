import pytest
from playwright.sync_api import Page, expect
import time
import subprocess
import os
import requests

# Server configuration
SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"


def test_value_in_node_persists_and_hides_cache(page: Page, sop_server):
    page.on("console", lambda msg: print(f"BROWSER CONSOLE: {msg.text}"))

    # Create store via API with ValueDataInNodeSegment=true
    test_store_name = f"vinode_{int(time.time())}"

    payload = {
        "database": "TestDB",
        "store": test_store_name,
        "key_type": "string",
        "value_type": "string",
        "description": "Value-in-node persisted",
        "index_spec": "",
        "cel_expression": "",
        "seed_key": "k1",
        "seed_value": "v1",
        # Intentionally omit advanced_mode; backend must not depend on it.
        "data_size": 0, # Small -> Value In Node = True
        # Even if provided, cache settings should be treated as not applicable when value-in-node is true.
        "cache_duration": 60,
        "is_cache_ttl": True,
    }

    resp = requests.post(f"{SERVER_URL}/api/store/add", json=payload)
    assert resp.status_code == 200, f"Failed to create store: {resp.text}"

    # Load UI and open edit modal
    page.goto(SERVER_URL)
    expect(page).to_have_title("SOP Data Manager")

    store_item = page.locator(f"#store-list li:has-text('{test_store_name}')")
    expect(store_item).to_be_visible(timeout=10000)
    store_item.click()

    edit_btn = page.locator("#btn-edit-store")
    expect(edit_btn).to_be_visible()
    edit_btn.click()

    expect(page.locator("#add-store-modal")).to_be_visible()

    # Data Size should reflect persisted storeinfo (Small/0 + disabled)
    ds = page.locator("#new-store-data-size")
    expect(ds).to_have_value("0")
    expect(ds).to_be_disabled()

    # Cache UI should be hidden when value-in-node is true (Small Data)
    cache_container = page.locator("#cache-options-container")
    expect(cache_container).to_be_hidden()
