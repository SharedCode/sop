import time

import pytest
import requests
from playwright.sync_api import Page, expect

# Server configuration
SERVER_PORT = 8080
SERVER_URL = f"http://localhost:{SERVER_PORT}"


def test_value_in_node_default_persists(page: Page, sop_server):
    # Create store via API with advanced_mode=false (matches the UI default path)
    store_name = f"vin_default_{int(time.time())}"

    payload = {
        "database": "TestDB",
        "store": store_name,
        "key_type": "string",
        "value_type": "string",
        "description": "VIN default",
        "index_spec": "",
        "cel_expression": "",
        "seed_key": None,
        "seed_value": None,
        # Advanced options explicitly disabled
        "advanced_mode": False,
        "slot_length": 1000,
        "is_unique": True,
        "is_value_data_in_node_segment": True,
        "cache_duration": 60,
        "is_cache_ttl": False,
    }

    r = requests.post(f"{SERVER_URL}/api/store/add", json=payload)
    assert r.status_code == 200, r.text

    page.goto(SERVER_URL)

    store_item = page.locator(f"#store-list li:has-text('{store_name}')")
    expect(store_item).to_be_visible(timeout=10000)
    store_item.click()

    edit_btn = page.locator("#btn-edit-store")
    expect(edit_btn).to_be_visible()
    edit_btn.click()

    # Data Size should reflect persisted state (Small/0) and cache options should be hidden
    ds = page.locator("#new-store-data-size")
    expect(ds).to_have_value("0")

    cache_container = page.locator("#cache-options-container")
    expect(cache_container).to_be_hidden()
